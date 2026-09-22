-- Persisted LCM store emitted by v0.1.0-beta.37.
--
-- This is intentionally a rowful fixture.  It contains the released LCM
-- tables, indexes, FTS tables and triggers, while the post-beta.37 queue,
-- invalidation work and predecessor-range tables are absent.  Migration tests
-- use the rows below to prove that adding those current objects is lossless.
PRAGMA foreign_keys = ON;

BEGIN;

CREATE TABLE sessions (
    provider TEXT NOT NULL,
    session_id TEXT NOT NULL,
    project_key TEXT NOT NULL,
    project_path TEXT NOT NULL,
    PRIMARY KEY(provider, session_id)
);

CREATE TABLE session_messages (
    provider TEXT NOT NULL,
    message_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    role TEXT NOT NULL,
    timestamp INTEGER,
    ordinal INTEGER NOT NULL,
    text TEXT NOT NULL,
    metadata_json TEXT,
    PRIMARY KEY(provider, message_id)
);

CREATE TABLE session_schema_migrations (
    name TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    applied_at INTEGER NOT NULL DEFAULT (unixepoch())
);

INSERT INTO sessions(provider, session_id, project_key, project_path)
VALUES ('cursor', 'beta37-session-a', 'project.beta37-a', '/projects/beta37-a'),
       ('claude', 'beta37-session-b', 'project.beta37-b', '/projects/beta37-b');

INSERT INTO session_messages(
    provider, message_id, session_id, role, timestamp, ordinal, text, metadata_json
)
VALUES ('cursor', 'legacy-beta37-a', 'beta37-session-a', 'user', 1740000000, 1,
        'legacy host row retained beside LCM', '{"release":"beta.37"}'),
       ('claude', 'legacy-beta37-b', 'beta37-session-b', 'assistant', 1740000001, 1,
        'legacy host row retained beside LCM', '{"release":"beta.37"}');

INSERT INTO session_schema_migrations(name, version, applied_at)
VALUES ('lcm', 8, 1710000000);

CREATE TABLE lcm_raw_messages (
    provider TEXT NOT NULL,
    message_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    store_id INTEGER PRIMARY KEY AUTOINCREMENT,
    role TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    timestamp INTEGER,
    content TEXT,
    content_hash TEXT NOT NULL,
    storage_kind TEXT NOT NULL CHECK(storage_kind IN ('inline', 'external')),
    payload_ref TEXT,
    snippet_text TEXT NOT NULL,
    index_text TEXT NOT NULL,
    legacy_source INTEGER NOT NULL DEFAULT 0,
    legacy_truncated INTEGER NOT NULL DEFAULT 0,
    metadata_json TEXT,
    UNIQUE(provider, message_id),
    FOREIGN KEY(provider, session_id)
        REFERENCES sessions(provider, session_id) ON DELETE CASCADE
);

CREATE INDEX idx_lcm_raw_session_order
    ON lcm_raw_messages(provider, session_id, store_id);
CREATE INDEX idx_lcm_raw_session_id
    ON lcm_raw_messages(session_id);

CREATE TABLE lcm_external_payloads (
    payload_ref TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    session_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    kind TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    byte_count INTEGER NOT NULL,
    char_count INTEGER NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    metadata_json TEXT,
    UNIQUE(provider, message_id, payload_ref),
    FOREIGN KEY(provider, session_id)
        REFERENCES sessions(provider, session_id) ON DELETE CASCADE
);

CREATE INDEX idx_lcm_external_payloads_owner
    ON lcm_external_payloads(provider, session_id);

CREATE TABLE lcm_gc_marks (
    payload_ref TEXT PRIMARY KEY,
    state TEXT NOT NULL CHECK(state IN ('unreferenced', 'missing')),
    first_seen_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL DEFAULT (unixepoch())
);

CREATE TABLE lcm_gc_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE lcm_summary_nodes (
    node_id TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    depth INTEGER NOT NULL,
    summary_text TEXT NOT NULL,
    summary_hash TEXT NOT NULL,
    summary_token_count INTEGER NOT NULL,
    source_token_count INTEGER NOT NULL,
    source_time_start INTEGER,
    source_time_end INTEGER,
    expand_hint TEXT,
    metadata_json TEXT,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    FOREIGN KEY(provider, session_id)
        REFERENCES sessions(provider, session_id) ON DELETE CASCADE
);

CREATE TABLE lcm_summary_sources (
    node_id TEXT NOT NULL,
    source_kind TEXT NOT NULL CHECK(source_kind IN ('raw_message', 'summary_node')),
    source_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL,
    PRIMARY KEY(node_id, ordinal),
    FOREIGN KEY(node_id) REFERENCES lcm_summary_nodes(node_id) ON DELETE CASCADE
);

CREATE INDEX idx_lcm_summary_nodes_session_depth_time
    ON lcm_summary_nodes(
        provider, session_id, depth, source_time_start, source_time_end, created_at
    );
CREATE INDEX idx_lcm_summary_nodes_codex_pending_session_order
    ON lcm_summary_nodes(
        session_id,
        (CASE
            WHEN json_valid(metadata_json) THEN
                json_extract(metadata_json, '$.source') = 'codex_context_compacted'
                AND COALESCE(
                      json_extract(metadata_json, '$.tracedecay_summary_source'),
                      ''
                    ) <> 'codex_app_server'
            ELSE 0
         END),
        depth DESC,
        created_at DESC,
        node_id
    )
    WHERE provider = 'codex';
CREATE INDEX idx_lcm_summary_nodes_codex_pending_root_order
    ON lcm_summary_nodes(
        (CASE
            WHEN json_valid(metadata_json) THEN
                json_extract(metadata_json, '$.source') = 'codex_context_compacted'
                AND COALESCE(
                      json_extract(metadata_json, '$.tracedecay_summary_source'),
                      ''
                    ) <> 'codex_app_server'
            ELSE 0
         END),
        created_at DESC,
        depth DESC,
        node_id,
        session_id
    )
    WHERE provider = 'codex';
CREATE INDEX idx_lcm_summary_sources_source
    ON lcm_summary_sources(source_kind, source_id);

CREATE TABLE lcm_lifecycle_state (
    provider TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    current_session_id TEXT NOT NULL,
    last_finalized_session_id TEXT,
    current_frontier_store_id INTEGER,
    last_finalized_frontier_store_id INTEGER,
    rollover_at INTEGER,
    reset_at INTEGER,
    maintenance_at INTEGER,
    boundary_skip_at INTEGER,
    updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY(provider, conversation_id)
);

CREATE TABLE lcm_maintenance_debt (
    provider TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    debt_id TEXT NOT NULL,
    debt_kind TEXT NOT NULL,
    from_store_id INTEGER,
    to_store_id INTEGER,
    metadata_json TEXT,
    created_at INTEGER NOT NULL DEFAULT (unixepoch()),
    PRIMARY KEY(provider, conversation_id, debt_id),
    FOREIGN KEY(provider, conversation_id)
        REFERENCES lcm_lifecycle_state(provider, conversation_id) ON DELETE CASCADE
);

CREATE INDEX idx_lcm_maintenance_debt_kind
    ON lcm_maintenance_debt(provider, debt_kind, created_at);

CREATE VIRTUAL TABLE lcm_raw_messages_fts USING fts5(
    index_text,
    content='lcm_raw_messages',
    content_rowid='store_id'
);
CREATE TRIGGER lcm_raw_messages_fts_insert
    AFTER INSERT ON lcm_raw_messages BEGIN
        INSERT INTO lcm_raw_messages_fts(rowid, index_text)
        VALUES (NEW.store_id, NEW.index_text);
    END;
CREATE TRIGGER lcm_raw_messages_fts_delete
    AFTER DELETE ON lcm_raw_messages BEGIN
        INSERT INTO lcm_raw_messages_fts(lcm_raw_messages_fts, rowid, index_text)
        VALUES ('delete', OLD.store_id, OLD.index_text);
    END;
CREATE TRIGGER lcm_raw_messages_fts_update
    AFTER UPDATE ON lcm_raw_messages BEGIN
        INSERT INTO lcm_raw_messages_fts(lcm_raw_messages_fts, rowid, index_text)
        VALUES ('delete', OLD.store_id, OLD.index_text);
        INSERT INTO lcm_raw_messages_fts(rowid, index_text)
        VALUES (NEW.store_id, NEW.index_text);
    END;

CREATE VIRTUAL TABLE lcm_summary_nodes_fts USING fts5(
    summary_text, expand_hint, metadata_json,
    content='lcm_summary_nodes',
    content_rowid='rowid'
);
CREATE TRIGGER lcm_summary_nodes_fts_insert
    AFTER INSERT ON lcm_summary_nodes BEGIN
        INSERT INTO lcm_summary_nodes_fts(rowid, summary_text, expand_hint, metadata_json)
        VALUES (NEW.rowid, NEW.summary_text, NEW.expand_hint, NEW.metadata_json);
    END;
CREATE TRIGGER lcm_summary_nodes_fts_delete
    AFTER DELETE ON lcm_summary_nodes BEGIN
        INSERT INTO lcm_summary_nodes_fts(
            lcm_summary_nodes_fts, rowid, summary_text, expand_hint, metadata_json
        )
        VALUES ('delete', OLD.rowid, OLD.summary_text, OLD.expand_hint, OLD.metadata_json);
    END;
CREATE TRIGGER lcm_summary_nodes_fts_update
    AFTER UPDATE ON lcm_summary_nodes BEGIN
        INSERT INTO lcm_summary_nodes_fts(
            lcm_summary_nodes_fts, rowid, summary_text, expand_hint, metadata_json
        )
        VALUES ('delete', OLD.rowid, OLD.summary_text, OLD.expand_hint, OLD.metadata_json);
        INSERT INTO lcm_summary_nodes_fts(rowid, summary_text, expand_hint, metadata_json)
        VALUES (NEW.rowid, NEW.summary_text, NEW.expand_hint, NEW.metadata_json);
    END;

INSERT INTO lcm_raw_messages(
    provider, message_id, session_id, store_id, role, ordinal, timestamp, content,
    content_hash, storage_kind, payload_ref, snippet_text, index_text,
    legacy_source, legacy_truncated, metadata_json
)
VALUES
    ('cursor', 'beta37-message-a1', 'beta37-session-a', 10, 'user', 1, 1740000010,
     'beta37 asks about retained context', 'a9c6933e38adaaa038912ceba2d539d6b8414eab9260409affd9589e5b200db5', 'inline', NULL,
     'beta37 asks about retained context', 'beta37 asks about retained context', 1, 0,
     '{"source":"beta.37","ingest_protection":{"sanitization_receipt":{"receipt":{"receipt_id":"privacy.lcm-payload.v1.612973383970d883a59d6af15af5b695b1fed68782758f4eda2bc61dd5a551df","sanitizer_version":"privacy.lcm-payload.v1"},"disposition":"accepted","sensitivity":"non_sensitive","payload":{"digest":"sha256:9d46adee8328293a92336dbfaf92a3ce8932bfe7faa8a9f9e1cbcef8289b0b7b","byte_len":36}}}}'),
    ('cursor', 'beta37-message-a2', 'beta37-session-a', 11, 'assistant', 2, 1740000011,
     'beta37 keeps this assistant answer lossless', 'c6d9d3f09091816c1e6cb617da60f46bc41fe725d6b955c11ad60756bdda6429', 'inline', NULL,
     'beta37 keeps this assistant answer lossless', 'beta37 keeps this assistant answer lossless', 1, 0,
     '{"source":"beta.37","metadata":{"turn":2},"ingest_protection":{"sanitization_receipt":{"receipt":{"receipt_id":"privacy.lcm-payload.v1.0de6a270ef5fbf44bb7b083e05f82a607433a8002b725a209d61d18376aa4a0f","sanitizer_version":"privacy.lcm-payload.v1"},"disposition":"accepted","sensitivity":"non_sensitive","payload":{"digest":"sha256:96e5954c981e75e5336e18793fc9c8ea45793365263ec0a54b37936ef5540b3c","byte_len":45}}}}'),
    ('claude', 'beta37-message-b1', 'beta37-session-b', 20, 'user', 1, 1740000020,
     NULL, '6fb23c7b89eb4d4363272981b1de5de6e0f66c57cfc2ae66273ef518ecbf6d8d', 'external', 'payload-beta37-b1',
     '[Externalized LCM payload: ref=payload-beta37-b1]',
     'beta37 external payload placeholder', 0, 0,
     '{"source":"beta.37","payload":{"kind":"tool-result"},"ingest_protection":{"sanitization_receipt":{"receipt":{"receipt_id":"privacy.lcm-payload.v1.a34cc66f6a184ad24c65d31bd6e5c7fb366313cd65c1945321ebe1354a5f95fc","sanitizer_version":"privacy.lcm-payload.v1"},"disposition":"accepted","sensitivity":"non_sensitive","payload":{"digest":"sha256:29b3f3d6557b1037ffa36d5f35a7a1ad6991327e0bf673adf062960b7dc61209","byte_len":37}}}}'),
    ('claude', 'beta37-message-b2', 'beta37-session-b', 21, 'assistant', 2, 1740000021,
     'beta37 assistant response with searchable text', '98ff0d1e10aace4115bfbe8d42664993d2b598240c638571b82e6e383d2f302b', 'inline', NULL,
     'beta37 assistant response with searchable text', 'beta37 assistant response with searchable text', 0, 1,
     '{"source":"beta.37","truncated":true,"ingest_protection":{"sanitization_receipt":{"receipt":{"receipt_id":"privacy.lcm-payload.v1.aa1dda7d223e30a15a85d7f18d817ba863726657f7e3d980b526642813276936","sanitizer_version":"privacy.lcm-payload.v1"},"disposition":"accepted","sensitivity":"non_sensitive","payload":{"digest":"sha256:9056309961b954317b3f1a99bcedb4a19da9507bd107588a928e0a1dede3af9b","byte_len":48}}}}');

INSERT INTO lcm_external_payloads(
    payload_ref, provider, session_id, message_id, kind, content_hash,
    byte_count, char_count, created_at, metadata_json
)
VALUES
    ('payload-beta37-b1', 'claude', 'beta37-session-b', 'beta37-message-b1',
     'tool-result', '6fb23c7b89eb4d4363272981b1de5de6e0f66c57cfc2ae66273ef518ecbf6d8d', 37, 37, 1740000030,
     '{"release":"beta.37","field_path":"content","sanitized":true}'),
    ('payload-beta37-orphan', 'claude', 'beta37-session-b', 'beta37-message-b2',
     'tool-result', '6fb23c7b89eb4d4363272981b1de5de6e0f66c57cfc2ae66273ef518ecbf6d8d', 37, 37, 1740000031,
     '{"release":"beta.37","orphan":true}');

INSERT INTO lcm_gc_marks(payload_ref, state, first_seen_at, updated_at)
VALUES ('payload-beta37-orphan', 'unreferenced', 1740000040, 1740000041),
       ('payload-beta37-missing', 'missing', 1740000042, 1740000043);

INSERT INTO lcm_gc_meta(key, value)
VALUES ('beta37-retention-frontier', '20'),
       ('beta37-custom-marker', '{"release":"beta.37"}');

INSERT INTO lcm_lifecycle_state(
    provider, conversation_id, current_session_id, last_finalized_session_id,
    current_frontier_store_id, last_finalized_frontier_store_id,
    rollover_at, reset_at, maintenance_at, boundary_skip_at, updated_at
)
VALUES ('cursor', 'beta37-conversation-a', 'beta37-session-a', NULL, 11, NULL,
        1740000050, NULL, 1740000051, 1740000052, 1740000053),
       ('claude', 'beta37-conversation-b', 'beta37-session-b', 'beta37-session-b', 21, 20,
        NULL, NULL, 1740000054, NULL, 1740000055);

INSERT INTO lcm_maintenance_debt(
    provider, conversation_id, debt_id, debt_kind, from_store_id, to_store_id,
    metadata_json, created_at
)
VALUES ('cursor', 'beta37-conversation-a', 'debt-beta37-a', 'summary', 10, 11,
        '{"release":"beta.37","reason":"pending summary"}', 1740000060),
       ('claude', 'beta37-conversation-b', 'debt-beta37-b', 'payload_gc', 20, 21,
        '{"release":"beta.37","reason":"orphan payload"}', 1740000061);

INSERT INTO lcm_summary_nodes(
    node_id, provider, conversation_id, session_id, depth, summary_text, summary_hash,
    summary_token_count, source_token_count, source_time_start, source_time_end,
    expand_hint, metadata_json, created_at
)
VALUES
    ('summary-beta37-root', 'cursor', 'beta37-conversation-a', 'beta37-session-a', 0,
     'beta37 root summary retains the original question', 'd2700f25c586a90cb518bc368c873cf3932aab71760998aa8a19facea1b4ebc4',
     8, 12, 1740000010, 1740000011, 'expand root',
     '{"source":"beta.37","tracedecay_summary_source":"release-fixture"}', 1740000070),
    ('summary-beta37-leaf', 'cursor', 'beta37-conversation-a', 'beta37-session-a', 1,
     'beta37 leaf summary retains assistant evidence', '1152525288b9dc10cf3c1cdc98c36b1aac562f044e7a8dcc3381c5b118198b4f',
     7, 9, 1740000011, 1740000011, 'expand leaf',
     '{"source":"beta.37","topic":"retained-context"}', 1740000071);

INSERT INTO lcm_summary_sources(node_id, source_kind, source_id, ordinal)
VALUES ('summary-beta37-root', 'raw_message', '10', 0),
       ('summary-beta37-leaf', 'summary_node', 'summary-beta37-root', 0),
       ('summary-beta37-leaf', 'raw_message', '11', 1);

COMMIT;
