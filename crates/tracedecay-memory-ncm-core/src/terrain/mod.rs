//! Three-dimensional diffusion, sampling, and Gaussian terrain writes.
//!
//! D09 uses position component `i` for grid axis `i`: `(x * G + y) * G + z`.
//! Affect is channel-major, matching the reference's `[1, 4, G, G, G]` tensor.
//! D01 applies a real normalized separable Gaussian before consolidation pours.

use crate::types::{AffectVector, CoreError, AFFECT_DIM, TERRAIN_DIM};
use serde::{Deserialize, Serialize};

/// Reference splat width in normalized position units.
pub const DEFAULT_SPLAT_SIGMA: f32 = 0.1;

/// Scalar intensity and four affect fields on a cubic grid.
///
/// Construction checks resolution and the combined explicit-step stability bound.
/// Public storage is for checkpoint/engine integration; callers modifying it must
/// preserve its lengths, finite values, and the validated coefficients.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Terrain3D {
    /// Grid side length, in `1..=64`.
    pub resolution: usize,
    /// Intensity, flattened as `(x * G + y) * G + z`.
    pub h: Vec<f32>,
    /// Affect, channel-major: `channel * G³ + (x * G + y) * G + z`.
    pub e: Vec<f32>,
    /// Intensity diffusion coefficient.
    pub alpha_h: f32,
    /// Affect diffusion coefficient.
    pub alpha_e: f32,
    /// Homeostatic leak toward intensity zero and affect one.
    pub leak: f32,
}

/// Reference terrain summary statistics.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct TerrainStats {
    /// Mean intensity.
    pub h_mean: f32,
    /// Maximum intensity.
    pub h_max: f32,
    /// Sample standard deviation (Bessel correction), zero for a single cell.
    pub h_std: f32,
    /// Cells whose intensity exceeds `1e-6`.
    pub h_nonzero: usize,
    /// Mean across all affect channels.
    pub e_mean: f32,
    /// Maximum absolute affect value.
    pub e_max: f32,
}

impl Terrain3D {
    /// Creates zero intensity and neutral affect with bounded, stable dynamics.
    ///
    /// Rejects non-finite/negative coefficients and `leak + 6 * alpha > 1`
    /// independently for H and E. Resolutions outside `1..=64` are rejected.
    pub fn new(
        resolution: usize,
        alpha_h: f32,
        alpha_e: f32,
        leak: f32,
    ) -> Result<Self, CoreError> {
        if !(1..=64).contains(&resolution) {
            return Err(CoreError::BudgetExceeded("terrain resolution (1..=64)"));
        }
        finite(&[alpha_h, alpha_e, leak], "terrain coefficients")?;
        if alpha_h < 0.0
            || alpha_e < 0.0
            || leak < 0.0
            || leak + 6.0 * alpha_h > 1.0
            || leak + 6.0 * alpha_e > 1.0
        {
            return Err(CoreError::InvalidState(
                "terrain requires nonnegative coefficients and leak + 6 * alpha <= 1".into(),
            ));
        }
        let cells = resolution.pow(3);
        Ok(Self {
            resolution,
            h: vec![0.0; cells],
            e: vec![1.0; AFFECT_DIM * cells],
            alpha_h,
            alpha_e,
            leak,
        })
    }

    /// Applies the six-neighbor replicate-boundary Laplacian and homeostasis.
    ///
    /// Every neighbor is read from the old generation, never from the output.
    /// H becomes `(1-leak)*H + alpha_h*lap(H)`; E additionally receives `leak`.
    /// Both results are clamped at zero, as in the reference.
    pub fn step(&mut self) {
        self.h = diffuse(&self.h, self.resolution, self.alpha_h, self.leak, 0.0);
        self.e = diffuse(&self.e, self.resolution, self.alpha_e, self.leak, self.leak);
    }

    /// Trilinearly samples finite normalized coordinates with border replication.
    ///
    /// `align_corners=true`: `g=(p+1)*0.5*(G-1)`, clamped to `[0,G-1]`.
    /// Unlike the reference `grid_sample` call, x and z are not reversed (D09).
    #[must_use]
    pub fn sample(&self, position: [f32; TERRAIN_DIM]) -> (f32, [f32; AFFECT_DIM]) {
        let last = self.resolution - 1;
        let grid = position.map(|p| ((p + 1.0) * 0.5 * last as f32).clamp(0.0, last as f32));
        let lower = grid.map(|p| p as usize);
        let upper = lower.map(|p| (p + 1).min(last));
        let fraction =
            std::array::from_fn::<_, TERRAIN_DIM, _>(|axis| grid[axis] - lower[axis] as f32);
        let mut h = 0.0;
        let mut e = [0.0; AFFECT_DIM];
        let cells = self.h.len();
        for dx in 0..2 {
            for dy in 0..2 {
                for dz in 0..2 {
                    let sides = [dx, dy, dz];
                    let mut coordinate = [0; TERRAIN_DIM];
                    let mut weight = 1.0;
                    for axis in 0..TERRAIN_DIM {
                        let high = sides[axis] == 1;
                        coordinate[axis] = if high { upper[axis] } else { lower[axis] };
                        weight *= if high {
                            fraction[axis]
                        } else {
                            1.0 - fraction[axis]
                        };
                    }
                    let index = flat(coordinate, self.resolution);
                    h += weight * self.h[index];
                    for (channel, value) in e.iter_mut().enumerate() {
                        *value += weight * self.e[channel * cells + index];
                    }
                }
            }
        }
        (h, e)
    }

    /// Adds the reference windowed Gaussian centered at the unrounded position.
    ///
    /// `sigma` is normalized (`DEFAULT_SPLAT_SIGMA` is 0.1), not in grid cells.
    /// The window center truncates toward zero and clamps to the grid; its radius
    /// is `max(2, int(3*sigma*G/2))`, capped at G (the same clipped window).
    /// `None` leaves E untouched, matching the reference's optional-emotion loop.
    /// Invalid inputs or overflowing updates fail without changing either field.
    pub fn splat(
        &mut self,
        position: [f32; TERRAIN_DIM],
        intensity: f32,
        affect: Option<[f32; AFFECT_DIM]>,
        sigma: f32,
        eta: f32,
    ) -> Result<(), CoreError> {
        finite(&position, "terrain position")?;
        finite(&[intensity, sigma, eta], "terrain splat")?;
        if let Some(values) = affect {
            AffectVector::validated(values)?;
        }
        if sigma <= 0.0 {
            return Err(CoreError::InvalidState(
                "terrain splat sigma must be positive".into(),
            ));
        }
        let g = self.resolution;
        let grid = position.map(|p| (p + 1.0) * 0.5 * (g - 1) as f32);
        let sigma_grid = sigma * g as f32 / 2.0;
        let denominator = 2.0 * sigma_grid.powi(2);
        let omega = intensity * eta;
        finite(&grid, "terrain grid position")?;
        finite(&[denominator, omega], "terrain splat scale")?;
        if denominator == 0.0 {
            return Err(CoreError::InvalidState(
                "terrain splat sigma underflow".into(),
            ));
        }
        // Rust float-to-usize saturates negatives to zero, equivalent here to
        // torch.long() (truncate toward zero) followed by clamp.
        let center = grid.map(|p| (p as usize).min(g - 1));
        let radius = ((3.0 * sigma * g as f32 / 2.0) as usize).max(2).min(g);
        let low = center.map(|c| c.saturating_sub(radius));
        let high = center.map(|c| (c + radius + 1).min(g));
        let cells = self.h.len();
        // Buffer only the bounded window so overflow cannot publish half a splat.
        let mut updates = Vec::new();
        for x in low[0]..high[0] {
            for y in low[1]..high[1] {
                for z in low[2]..high[2] {
                    let dist_sq = (x as f32 - grid[0]).powi(2)
                        + (y as f32 - grid[1]).powi(2)
                        + (z as f32 - grid[2]).powi(2);
                    let delta = omega * (-dist_sq / denominator).exp();
                    let index = flat([x, y, z], g);
                    let next_h = self.h[index] + delta;
                    let mut next_e = [0.0; AFFECT_DIM];
                    for (channel, value) in next_e.iter_mut().enumerate() {
                        *value = self.e[channel * cells + index];
                        if let Some(values) = affect {
                            *value += delta * values[channel];
                        }
                    }
                    finite(&[next_h], "terrain splat H update")?;
                    finite(&next_e, "terrain splat E update")?;
                    updates.push((index, next_h, next_e));
                }
            }
        }
        for (index, next_h, next_e) in updates {
            self.h[index] = next_h;
            for (channel, value) in next_e.into_iter().enumerate() {
                self.e[channel * cells + index] = value;
            }
        }
        Ok(())
    }

    /// Returns a real separable Gaussian convolution, without mutating the source.
    ///
    /// The normalized 1-D kernel has `max(3, int(6*sigma_cells)|1)` taps.
    /// Passes run x, then y, then z, with replicate padding, on H and every E
    /// channel. Sigma must be finite and positive, with at most 385 kernel taps.
    pub fn blur(&self, sigma_cells: f32) -> Result<(Vec<f32>, Vec<f32>), CoreError> {
        let kernel = gaussian_kernel(sigma_cells)?;
        Ok((
            blur_field(&self.h, self.resolution, &kernel),
            blur_field(&self.e, self.resolution, &kernel),
        ))
    }

    /// Adds `xi_h * blur(other.H)` and `xi_e * blur(other.E)`, including neutral E.
    ///
    /// Mismatched grids, invalid blur parameters, and non-finite results are
    /// rejected before changing the destination; the source is never changed.
    pub fn merge_from(
        &mut self,
        other: &Self,
        xi_h: f32,
        xi_e: f32,
        blur_sigma: f32,
    ) -> Result<(), CoreError> {
        if self.resolution != other.resolution {
            return Err(CoreError::DimensionMismatch {
                what: "terrain resolution",
                expected: self.resolution,
                actual: other.resolution,
            });
        }
        finite(&[xi_h, xi_e], "terrain merge weights")?;
        let (mut h, mut e) = other.blur(blur_sigma)?;
        for (value, old) in h.iter_mut().zip(&self.h) {
            *value = *old + xi_h * *value;
        }
        for (value, old) in e.iter_mut().zip(&self.e) {
            *value = *old + xi_e * *value;
        }
        finite(&h, "terrain merge H")?;
        finite(&e, "terrain merge E")?;
        self.h = h;
        self.e = e;
        Ok(())
    }

    /// Restores zero intensity and neutral affect without changing coefficients.
    pub fn reset(&mut self) {
        self.h.fill(0.0);
        self.e.fill(1.0);
    }

    /// Computes the reference summary, using sample standard deviation for H.
    #[must_use]
    pub fn stats(&self) -> TerrainStats {
        let h_mean = self.h.iter().sum::<f32>() / self.h.len() as f32;
        let variance = self.h.iter().map(|v| (v - h_mean).powi(2)).sum::<f32>()
            / self.h.len().saturating_sub(1).max(1) as f32;
        TerrainStats {
            h_mean,
            h_max: self.h.iter().copied().fold(f32::NEG_INFINITY, f32::max),
            h_std: variance.sqrt(),
            h_nonzero: self.h.iter().filter(|v| **v > 1e-6).count(),
            e_mean: self.e.iter().sum::<f32>() / self.e.len() as f32,
            e_max: self.e.iter().map(|v| v.abs()).fold(0.0, f32::max),
        }
    }

    /// FNV-1a digest of little-endian H then channel-major E field bytes.
    ///
    /// This deterministic test fingerprint is not a cryptographic identity.
    #[must_use]
    pub fn digest(&self) -> u64 {
        self.h
            .iter()
            .chain(&self.e)
            .flat_map(|v| v.to_le_bytes())
            .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
                (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
            })
    }
}

fn finite(values: &[f32], what: &'static str) -> Result<(), CoreError> {
    if values.iter().all(|v| v.is_finite()) {
        Ok(())
    } else {
        Err(CoreError::NonFinite(what))
    }
}

fn flat([x, y, z]: [usize; TERRAIN_DIM], g: usize) -> usize {
    (x * g + y) * g + z
}

fn diffuse(old: &[f32], g: usize, alpha: f32, leak: f32, neutral_leak: f32) -> Vec<f32> {
    let cells = g.pow(3);
    let mut next = vec![0.0; old.len()];
    for (src, dst) in old.chunks_exact(cells).zip(next.chunks_exact_mut(cells)) {
        for x in 0..g {
            for y in 0..g {
                for z in 0..g {
                    let index = flat([x, y, z], g);
                    // Nonzero kernel taps in conv3d's lexicographic order.
                    let lap = src[flat([x.saturating_sub(1), y, z], g)]
                        + src[flat([x, y.saturating_sub(1), z], g)]
                        + src[flat([x, y, z.saturating_sub(1)], g)]
                        - 6.0 * src[index]
                        + src[flat([x, y, (z + 1).min(g - 1)], g)]
                        + src[flat([x, (y + 1).min(g - 1), z], g)]
                        + src[flat([(x + 1).min(g - 1), y, z], g)];
                    dst[index] = ((1.0 - leak) * src[index] + neutral_leak + alpha * lap).max(0.0);
                }
            }
        }
    }
    next
}

fn gaussian_kernel(sigma: f32) -> Result<Vec<f32>, CoreError> {
    finite(&[sigma], "terrain blur sigma")?;
    if sigma <= 0.0 || 2.0 * sigma.powi(2) == 0.0 {
        return Err(CoreError::InvalidState(
            "terrain blur sigma must be positive and representable".into(),
        ));
    }
    if sigma > 64.0 {
        return Err(CoreError::BudgetExceeded("terrain blur kernel (385 taps)"));
    }
    let size = ((6.0 * sigma) as usize | 1).max(3);
    let mut kernel: Vec<f32> = (0..size)
        .map(|i| {
            let offset = i as f32 - (size / 2) as f32;
            (-offset.powi(2) / (2.0 * sigma.powi(2))).exp()
        })
        .collect();
    let sum = kernel.iter().sum::<f32>();
    for weight in &mut kernel {
        *weight /= sum;
    }
    Ok(kernel)
}

fn blur_field(field: &[f32], g: usize, kernel: &[f32]) -> Vec<f32> {
    let cells = g.pow(3);
    let radius = kernel.len() / 2;
    let mut source = field.to_vec();
    let mut target = vec![0.0; field.len()];
    for axis in 0..TERRAIN_DIM {
        for (src, dst) in source
            .chunks_exact(cells)
            .zip(target.chunks_exact_mut(cells))
        {
            for x in 0..g {
                for y in 0..g {
                    for z in 0..g {
                        let mut sum = 0.0;
                        for (tap, weight) in kernel.iter().enumerate() {
                            let mut coordinate = [x, y, z];
                            coordinate[axis] =
                                (coordinate[axis] + tap).saturating_sub(radius).min(g - 1);
                            sum += weight * src[flat(coordinate, g)];
                        }
                        dst[flat([x, y, z], g)] = sum;
                    }
                }
            }
        }
        std::mem::swap(&mut source, &mut target);
    }
    source
}
