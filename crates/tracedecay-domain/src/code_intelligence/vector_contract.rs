use crate::{CodeSearchChunkId, ContentDigest, DomainError, ProjectionKeyV1, canonical_sha256};

const VECTOR_OUTPUT_DIGEST_DOMAIN: &str = "tracedecay.semantic-vector-output.v1";

/// Hash the exact floating-point payload emitted for one admitted vector.
///
/// Vector bytes are part of the immutable projection evidence. Hashing their
/// IEEE-754 bit patterns keeps the identity deterministic across serialization
/// boundaries while retaining the domain's existing non-finite-value checks.
pub fn semantic_vector_output_digest(
    projection_key: &ProjectionKeyV1,
    chunk_id: &CodeSearchChunkId,
    chunk_digest: &ContentDigest,
    values: &[f32],
) -> Result<ContentDigest, DomainError> {
    let bits = values.iter().map(|value| value.to_bits()).collect::<Vec<_>>();
    let digest = canonical_sha256(&(
        VECTOR_OUTPUT_DIGEST_DOMAIN,
        projection_key,
        chunk_id,
        chunk_digest,
        bits,
    ))?;
    ContentDigest::new(digest.as_str().to_owned())
}
