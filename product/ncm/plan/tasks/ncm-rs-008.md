# ncm-rs-008 — Implement actual 3D terrain, diffusion, sampling and Gaussian blur

Status: planned, not executed.
Owner: Core / terrain.
Dependencies: ncm-rs-002, ncm-rs-004.
External gates: none.

## Objective

Provide the spatial dynamics that the reference describes, including a real blur implementation.

## Owned paths

- `crates/tracedecay-memory-ncm-core/src/terrain/`
- `crates/tracedecay-memory-ncm-core/tests/terrain.rs`

## Implementation

1. Implement two 48³ fields with scalar H and four E channels, six-neighbor Laplacian, replicated boundaries, Gaussian splat, trilinear sampling and homeostasis.
2. Implement a real separable Gaussian blur with documented radius, normalization and boundary policy. Do not port Terrain3D.blur’s unchanged clones.
3. Audit torch grid_sample coordinate ordering against tensor layout and splat indexing. Choose one coherent v1 axis convention and record any reference deviation.
4. Use double buffers or immutable generations for diffusion; in-place neighbor sweeps must not introduce order-dependent math. Validate combined leak/diffusion coefficients and bounded work.

## Acceptance

1. An off-center asymmetric impulse spreads under blur; a constant field remains constant under normalized blur; interior impulse mass and symmetry match the chosen kernel.
2. Sampling a splatted asymmetric point catches x/z or stride swaps; test corners, faces and fractional positions.
3. H decays toward 0 and E toward 1 without NaN/negative excursions beyond the declared clamp policy. For the explicit combined update, require a sufficient stability bound such as lambda+6*alpha<=1.
4. Corrected blur fails against the original no-op output for the intended reason; all unchanged operations meet oracle tolerances.

## Verification targets

1. cargo test -p tracedecay-memory-ncm-core --test terrain --locked

## Sources

- [S04: Biomem terrain](https://github.com/BleedingDev/biomem/blob/500847ff65b5d9548b3826fa29bf3ccf8d221147/src/memory_module/terrain_3d.py)

## Handoff

Commit one reviewable slice; attach the common completion receipt; do not change shared host paths outside the ownership agreement.

Use the common evidence receipt and ownership rules in `START_HERE.md`. These are task specifications, not claims that the test targets already exist.


---
