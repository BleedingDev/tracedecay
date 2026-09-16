-- Data portion of a beta37 configuration snapshot. The schema fixture is
-- loaded first; this snapshot was written while registry revision 4 still
-- admitted semantic.runtime.v1 and query.default_collection.v1. Revision 7
-- adds the memory-provider settings, so those entries are deliberately absent
-- and must not be manufactured by a physical schema cutover.
INSERT INTO configuration_revisions (
    revision_id,
    parent_revision_id,
    snapshot_id,
    effective_behavior_digest,
    resolution_provenance_digest,
    actor_id,
    operation_kind,
    created_at
) VALUES (
    'revision.beta37.registry4',
    NULL,
    'snapshot.beta37.registry4',
    'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
    'sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',
    'actor.beta37',
    'canonical_initialization',
    37
);

INSERT INTO configuration_entries (
    revision_id,
    key,
    layer_kind,
    layer_id,
    schema_revision,
    typed_value
) VALUES
    (
        'revision.beta37.registry4',
        'analyzer.settings.v1',
        'default',
        NULL,
        1,
        '{"schema_version":1,"value":{"kind":"analyzer_settings","value":{"schema_version":1,"selections":[]}},"provenance":[{"layer":{"kind":"default"},"revision_id":"configuration.registry.default.v1","disposition":"defaulted","safe_reason":"registry_default"}]}'
    ),
    (
        'revision.beta37.registry4',
        'query.default_collection.v1',
        'default',
        NULL,
        1,
        '{"schema_version":1,"value":{"kind":"default_collection","value":null},"provenance":[{"layer":{"kind":"default"},"revision_id":"configuration.registry.default.v1","disposition":"defaulted","safe_reason":"registry_default"}]}'
    ),
    (
        'revision.beta37.registry4',
        'semantic.runtime.v1',
        'default',
        NULL,
        1,
        '{"schema_version":1,"value":{"kind":"text","value":"{\"selected_model\":\"JinaEmbeddingsV2BaseCode\",\"auto_download\":true,\"active_profile\":null,\"rollback_profile\":null,\"resources\":{\"max_model_bytes\":734003200,\"max_tokenizer_bytes\":67108864,\"max_resident_bytes\":2147483648,\"max_threads\":4,\"max_concurrent_sessions\":16,\"max_batch_size\":32,\"max_sequence_length\":512,\"load_deadline_ms\":30000}}"},"provenance":[{"layer":{"kind":"default"},"revision_id":"configuration.registry.default.v1","disposition":"defaulted","safe_reason":"registry_default"}]}'
    );
