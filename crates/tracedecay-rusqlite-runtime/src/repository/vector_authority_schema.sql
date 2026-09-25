CREATE TABLE IF NOT EXISTS vector_authority_heads_v1 (
    authority_namespace TEXT PRIMARY KEY NOT NULL CHECK (length(authority_namespace) > 0),
    revision INTEGER NOT NULL CHECK (revision >= 0),
    active_generation_id TEXT,
    rollback_generation_id TEXT,
    CHECK (active_generation_id IS NULL
        OR rollback_generation_id IS NULL
        OR active_generation_id <> rollback_generation_id),
    FOREIGN KEY (active_generation_id)
        REFERENCES vector_authority_generations_v1(generation_id) ON DELETE RESTRICT,
    FOREIGN KEY (rollback_generation_id)
        REFERENCES vector_authority_generations_v1(generation_id) ON DELETE RESTRICT
) STRICT;

CREATE TABLE IF NOT EXISTS vector_authority_stages_v1 (
    authority_namespace TEXT NOT NULL
        REFERENCES vector_authority_heads_v1(authority_namespace) ON DELETE CASCADE,
    build_id TEXT NOT NULL,
    projection_key TEXT NOT NULL,
    plan_digest TEXT NOT NULL,
    canonical_plan BLOB NOT NULL CHECK (length(canonical_plan) > 0),
    checkpoint BLOB NOT NULL CHECK (length(checkpoint) > 0),
    PRIMARY KEY (authority_namespace),
    UNIQUE (authority_namespace, build_id)
) STRICT;

CREATE TABLE IF NOT EXISTS vector_authority_batches_v1 (
    authority_namespace TEXT NOT NULL,
    build_id TEXT NOT NULL,
    batch_ordinal INTEGER NOT NULL CHECK (batch_ordinal >= 0),
    request_digest TEXT NOT NULL,
    prepared_digest TEXT NOT NULL,
    prior_checkpoint BLOB,
    committed_checkpoint BLOB NOT NULL CHECK (length(committed_checkpoint) > 0),
    canonical_prepared BLOB NOT NULL CHECK (length(canonical_prepared) > 0),
    PRIMARY KEY (authority_namespace, build_id, batch_ordinal),
    UNIQUE (authority_namespace, build_id, request_digest),
    FOREIGN KEY (authority_namespace, build_id)
        REFERENCES vector_authority_stages_v1(authority_namespace, build_id) ON DELETE CASCADE
) STRICT;

CREATE TABLE IF NOT EXISTS vector_authority_generations_v1 (
    generation_id TEXT PRIMARY KEY NOT NULL,
    projection_key TEXT NOT NULL,
    manifest_digest TEXT NOT NULL,
    canonical_generation BLOB NOT NULL CHECK (length(canonical_generation) > 0)
) STRICT;

CREATE TABLE IF NOT EXISTS vector_authority_float_blobs_v1 (
    output_digest TEXT PRIMARY KEY NOT NULL,
    dimensions INTEGER NOT NULL CHECK (dimensions > 0),
    byte_length INTEGER NOT NULL CHECK (
        byte_length = length(canonical_f32_le)
        AND byte_length = dimensions * 4
    ),
    canonical_f32_le BLOB NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS vector_authority_generation_blobs_v1 (
    generation_id TEXT NOT NULL
        REFERENCES vector_authority_generations_v1(generation_id) ON DELETE CASCADE,
    output_digest TEXT NOT NULL
        REFERENCES vector_authority_float_blobs_v1(output_digest) ON DELETE RESTRICT,
    PRIMARY KEY (generation_id, output_digest)
) STRICT;
