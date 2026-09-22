use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
use crate::retrieval_content::projected_content_hash;
#[cfg(test)]
use tracedecay_runtime_core::db::engine::{Connection, TransactionBehavior};
use tracedecay_runtime_core::db::engine::{Executor, QueryExecutor, params};

use super::{LcmError, LcmRawMessage, raw};

#[cfg(test)]
use super::util;

pub const LCM_SCHEMA_VERSION: i64 = 8;

const MIGRATION_NAME: &str = "lcm";

/// Indexes that keep expensive LCM reads off the message-body table pages.
///
/// `lcm_status` aggregates whole-store counts on every probe. Without these
/// indexes four of its components scan the full `lcm_raw_messages` /
/// `lcm_summary_nodes` / `lcm_external_payloads` records — multi-gigabyte
/// body reads on a long-lived profile store for a one-row answer (issue #767
/// measured 10.65 s daemon-side). Each entry is one independently committed
/// idempotent batch. Fresh stores and admitted current stores install the
/// final index shape synchronously with the schema. The one-time work is one
/// full-table build per missing index instead of that same scan on every
/// status call.
///
/// Each partial-index predicate must stay byte-identical to the query term
/// that relies on it: SQLite substitutes a partial index only when its query
/// terms structurally imply the index's WHERE clause. The status predicates
/// live in [`super::query`]; the raw direct-user candidate predicate lives in
/// [`super::query::grep`].
pub const LCM_STATUS_PERFORMANCE_INDEX_SQL: &[&str] = &[
    "CREATE INDEX IF NOT EXISTS idx_lcm_raw_legacy_truncated
         ON lcm_raw_messages(provider, session_id)
         WHERE legacy_truncated != 0;",
    "CREATE INDEX IF NOT EXISTS idx_lcm_raw_lossy_ingest
         ON lcm_raw_messages(provider, session_id)
         WHERE metadata_json IS NOT NULL
           AND json_valid(metadata_json)
           AND json_type(metadata_json, '$.ingest_protection.lossy') = 'true';",
    // Raw LIKE retrieval must retain infix and lossless-content semantics, so
    // FTS cannot be its candidate authority. Direct-user retrieval instead
    // admits the complete `role = 'user'` superset through this narrow index,
    // then applies the metadata-sensitive tool-result exclusion exactly over
    // that bounded set.
    "CREATE INDEX IF NOT EXISTS idx_lcm_raw_direct_user_candidate
         ON lcm_raw_messages(provider, store_id)
         WHERE role = 'user';",
    "CREATE INDEX IF NOT EXISTS idx_lcm_summary_nodes_depth_tokens
         ON lcm_summary_nodes(
             provider, session_id, depth, summary_token_count, source_token_count
         );",
    // The byte-count variant covers the status COUNT+SUM without touching
    // payload metadata rows and fully supersedes the plain owner index
    // (same leading columns), so the replacement and the drop commit as one
    // batch and no scope is ever left without an owner index.
    "CREATE INDEX IF NOT EXISTS idx_lcm_external_payloads_owner_bytes
         ON lcm_external_payloads(provider, session_id, byte_count);
     DROP INDEX IF EXISTS idx_lcm_external_payloads_owner;",
];

/// Raw-message FTS structure (schema v3): index only `index_text`, matching
/// hermes-lcm `build_message_fts_spec` (store.py:173-204), which indexes
/// nothing but the message content column. Earlier schemas also indexed
/// `role` and `metadata_json`, so an unqualified MATCH over-matched rows via
/// role names or metadata text. Role and source filtering happen as plain
/// SQL predicates on `lcm_raw_messages`, never through the FTS index.
const RAW_FTS_DDL: &str = "CREATE VIRTUAL TABLE IF NOT EXISTS lcm_raw_messages_fts USING fts5(
        index_text,
        content='lcm_raw_messages',
        content_rowid='store_id'
    );
    CREATE TRIGGER IF NOT EXISTS lcm_raw_messages_fts_insert
        AFTER INSERT ON lcm_raw_messages BEGIN
            INSERT INTO lcm_raw_messages_fts(rowid, index_text)
            VALUES (NEW.store_id, NEW.index_text);
        END;
    CREATE TRIGGER IF NOT EXISTS lcm_raw_messages_fts_delete
        AFTER DELETE ON lcm_raw_messages BEGIN
            INSERT INTO lcm_raw_messages_fts(lcm_raw_messages_fts, rowid, index_text)
            VALUES ('delete', OLD.store_id, OLD.index_text);
        END;
    CREATE TRIGGER IF NOT EXISTS lcm_raw_messages_fts_update
        AFTER UPDATE ON lcm_raw_messages BEGIN
            INSERT INTO lcm_raw_messages_fts(lcm_raw_messages_fts, rowid, index_text)
            VALUES ('delete', OLD.store_id, OLD.index_text);
            INSERT INTO lcm_raw_messages_fts(rowid, index_text)
            VALUES (NEW.store_id, NEW.index_text);
        END;";

const RAW_FTS_OBJECT_DEFINITIONS: &[(&str, &str, &str)] = &[
    (
        "lcm_raw_messages_fts",
        "table",
        "CREATE VIRTUAL TABLE lcm_raw_messages_fts USING fts5(
            index_text,
            content='lcm_raw_messages',
            content_rowid='store_id'
        )",
    ),
    (
        "lcm_raw_messages_fts_insert",
        "trigger",
        "CREATE TRIGGER lcm_raw_messages_fts_insert
            AFTER INSERT ON lcm_raw_messages BEGIN
                INSERT INTO lcm_raw_messages_fts(rowid, index_text)
                VALUES (NEW.store_id, NEW.index_text);
            END",
    ),
    (
        "lcm_raw_messages_fts_delete",
        "trigger",
        "CREATE TRIGGER lcm_raw_messages_fts_delete
            AFTER DELETE ON lcm_raw_messages BEGIN
                INSERT INTO lcm_raw_messages_fts(lcm_raw_messages_fts, rowid, index_text)
                VALUES ('delete', OLD.store_id, OLD.index_text);
            END",
    ),
    (
        "lcm_raw_messages_fts_update",
        "trigger",
        "CREATE TRIGGER lcm_raw_messages_fts_update
            AFTER UPDATE ON lcm_raw_messages BEGIN
                INSERT INTO lcm_raw_messages_fts(lcm_raw_messages_fts, rowid, index_text)
                VALUES ('delete', OLD.store_id, OLD.index_text);
                INSERT INTO lcm_raw_messages_fts(rowid, index_text)
                VALUES (NEW.store_id, NEW.index_text);
            END",
    ),
];

const SUMMARY_FTS_OBJECT_DEFINITIONS: &[(&str, &str, &str)] = &[
    (
        "lcm_summary_nodes_fts",
        "table",
        "CREATE VIRTUAL TABLE lcm_summary_nodes_fts USING fts5(
            summary_text, expand_hint, metadata_json,
            content='lcm_summary_nodes',
            content_rowid='rowid'
        )",
    ),
    (
        "lcm_summary_nodes_fts_insert",
        "trigger",
        "CREATE TRIGGER lcm_summary_nodes_fts_insert
            AFTER INSERT ON lcm_summary_nodes BEGIN
                INSERT INTO lcm_summary_nodes_fts(rowid, summary_text, expand_hint, metadata_json)
                VALUES (NEW.rowid, NEW.summary_text, NEW.expand_hint, NEW.metadata_json);
            END",
    ),
    (
        "lcm_summary_nodes_fts_delete",
        "trigger",
        "CREATE TRIGGER lcm_summary_nodes_fts_delete
            AFTER DELETE ON lcm_summary_nodes BEGIN
                INSERT INTO lcm_summary_nodes_fts(
                    lcm_summary_nodes_fts, rowid, summary_text, expand_hint, metadata_json
                )
                VALUES ('delete', OLD.rowid, OLD.summary_text, OLD.expand_hint, OLD.metadata_json);
            END",
    ),
    (
        "lcm_summary_nodes_fts_update",
        "trigger",
        "CREATE TRIGGER lcm_summary_nodes_fts_update
            AFTER UPDATE ON lcm_summary_nodes BEGIN
                INSERT INTO lcm_summary_nodes_fts(
                    lcm_summary_nodes_fts, rowid, summary_text, expand_hint, metadata_json
                )
                VALUES ('delete', OLD.rowid, OLD.summary_text, OLD.expand_hint, OLD.metadata_json);
                INSERT INTO lcm_summary_nodes_fts(rowid, summary_text, expand_hint, metadata_json)
                VALUES (NEW.rowid, NEW.summary_text, NEW.expand_hint, NEW.metadata_json);
            END",
    ),
];

/// Returns whether the raw-message FTS table and all three synchronization
/// triggers use the v3 content-only contracts.
pub async fn raw_fts_structure_is_current(conn: &(impl QueryExecutor + ?Sized)) -> Option<bool> {
    for (name, object_type, expected_sql) in RAW_FTS_OBJECT_DEFINITIONS {
        let object = conn
            .query(
                "SELECT type, COALESCE(sql, '')
                 FROM sqlite_master WHERE name = ?1",
                params![*name],
            )
            .await
            .ok()?;
        let mut rows = object;
        let row = rows.next().await.ok()??;
        let actual_type: String = row.get(0).ok()?;
        let actual_sql: String = row.get(1).ok()?;
        if actual_type != *object_type || compact_sql(&actual_sql) != compact_sql(expected_sql) {
            return Some(false);
        }
    }
    Some(true)
}

fn compact_sql(sql: &str) -> String {
    let mut compact = String::with_capacity(sql.len());
    let mut quoted = None;
    for character in sql.chars() {
        if let Some(quote) = quoted {
            compact.push(character);
            if character == quote {
                quoted = None;
            }
        } else if matches!(character, '\'' | '"' | '`') {
            quoted = Some(character);
            compact.push(character);
        } else if character.is_ascii_whitespace() {
            continue;
        } else {
            compact.push(character.to_ascii_lowercase());
        }
    }
    compact
}

/// Drops any existing raw-message FTS table/triggers (old or new shape),
/// recreates the v3 content-only structure, and repopulates the index from
/// `lcm_raw_messages` via the FTS5 `'rebuild'` command. Used by the schema
/// explicit schema initialization/rebuild owner; idempotent and data-preserving
/// because the index is derived entirely from the content table. Doctor never
/// invokes this mutation.
pub async fn rebuild_raw_fts(conn: &(impl Executor + ?Sized)) -> Option<()> {
    conn.execute_batch(
        "DROP TRIGGER IF EXISTS lcm_raw_messages_fts_insert;
         DROP TRIGGER IF EXISTS lcm_raw_messages_fts_delete;
         DROP TRIGGER IF EXISTS lcm_raw_messages_fts_update;
         DROP TABLE IF EXISTS lcm_raw_messages_fts;",
    )
    .await
    .ok()?;
    conn.execute_batch(RAW_FTS_DDL).await.ok()?;
    conn.execute(
        "INSERT INTO lcm_raw_messages_fts(lcm_raw_messages_fts) VALUES('rebuild')",
        (),
    )
    .await
    .ok()?;
    Some(())
}

/// Test-only convenience wrapper: production schema creation runs through
/// [`ensure_lcm_schema_in_transaction`] inside the callers' own transactions.
#[cfg(test)]
pub async fn ensure_lcm_schema(conn: &Connection) -> Result<(), LcmError> {
    let transaction = conn
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .await?;
    match ensure_lcm_schema_in_transaction(&transaction).await {
        Ok(()) => transaction.commit().await.map_err(Into::into),
        Err(error) => match transaction.rollback().await {
            Ok(()) => Err(error),
            Err(rollback_error) => Err(LcmError::Db(format!(
                "{error}; rollback after LCM schema migration failed: {rollback_error}"
            ))),
        },
    }
}

/// LCM schema state of a profile store that may be admitted without a reset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LcmSchemaAdmission {
    /// The store already carries the exact current LCM schema.
    Current,
    /// The store carries the exact LCM schema published by beta.37.
    ReleasedBeta37,
    /// The store carries no LCM schema and no legacy session content, so the
    /// current schema may be installed.
    Fresh,
}

const LCM_BETA37_SCHEMA_OBJECTS: &[&str] = &[
    "idx_lcm_external_payloads_owner",
    "idx_lcm_maintenance_debt_kind",
    "idx_lcm_raw_session_id",
    "idx_lcm_raw_session_order",
    "idx_lcm_summary_nodes_codex_pending_root_order",
    "idx_lcm_summary_nodes_codex_pending_session_order",
    "idx_lcm_summary_nodes_session_depth_time",
    "idx_lcm_summary_sources_source",
    "lcm_external_payloads",
    "lcm_gc_marks",
    "lcm_gc_meta",
    "lcm_lifecycle_state",
    "lcm_maintenance_debt",
    "lcm_raw_messages",
    "lcm_raw_messages_fts",
    "lcm_raw_messages_fts_config",
    "lcm_raw_messages_fts_data",
    "lcm_raw_messages_fts_docsize",
    "lcm_raw_messages_fts_idx",
    "lcm_raw_messages_fts_delete",
    "lcm_raw_messages_fts_insert",
    "lcm_raw_messages_fts_update",
    "lcm_summary_nodes",
    "lcm_summary_nodes_fts",
    "lcm_summary_nodes_fts_config",
    "lcm_summary_nodes_fts_data",
    "lcm_summary_nodes_fts_docsize",
    "lcm_summary_nodes_fts_idx",
    "lcm_summary_nodes_fts_delete",
    "lcm_summary_nodes_fts_insert",
    "lcm_summary_nodes_fts_update",
    "lcm_summary_sources",
];

const LCM_CURRENT_SCHEMA_OBJECTS: &[&str] = &[
    "idx_lcm_external_payloads_owner_bytes",
    "idx_lcm_maintenance_debt_kind",
    "idx_lcm_raw_direct_user_candidate",
    "idx_lcm_raw_legacy_truncated",
    "idx_lcm_raw_lossy_ingest",
    "idx_lcm_raw_predecessor_session_to",
    "idx_lcm_raw_session_id",
    "idx_lcm_raw_session_order",
    "idx_lcm_summary_convergence_due",
    "idx_lcm_summary_nodes_codex_pending_root_order",
    "idx_lcm_summary_nodes_codex_pending_session_order",
    "idx_lcm_summary_nodes_depth_tokens",
    "idx_lcm_summary_nodes_session_depth_time",
    "idx_lcm_summary_sources_source",
    "idx_lcm_summary_sources_source_node",
    "lcm_external_payloads",
    "lcm_gc_marks",
    "lcm_gc_meta",
    "lcm_lifecycle_state",
    "lcm_maintenance_debt",
    "lcm_raw_messages",
    "lcm_raw_messages_fts",
    "lcm_raw_messages_fts_config",
    "lcm_raw_messages_fts_data",
    "lcm_raw_messages_fts_docsize",
    "lcm_raw_messages_fts_idx",
    "lcm_raw_messages_fts_delete",
    "lcm_raw_messages_fts_insert",
    "lcm_raw_messages_fts_update",
    "lcm_raw_predecessor_ranges",
    "lcm_summary_convergence_dirty_raw",
    "lcm_summary_convergence_invalidation_work",
    "lcm_summary_convergence_queue",
    "lcm_summary_nodes",
    "lcm_summary_nodes_fts",
    "lcm_summary_nodes_fts_config",
    "lcm_summary_nodes_fts_data",
    "lcm_summary_nodes_fts_docsize",
    "lcm_summary_nodes_fts_idx",
    "lcm_summary_nodes_fts_delete",
    "lcm_summary_nodes_fts_insert",
    "lcm_summary_nodes_fts_update",
    "lcm_summary_sources",
    "lcm_summary_convergence_dirty_raw_seed",
    "lcm_summary_convergence_raw_insert",
    "lcm_summary_convergence_raw_unprotected_update",
];

// A view may have an arbitrary name and still expose LCM-owned rows. Keep the
// table identifier list explicit so admission catches those references while
// avoiding false positives for unrelated SQL that merely contains an `lcm_`
// substring.
const LCM_TABLE_REFERENCE_NAMES: &[&str] = &[
    "lcm_external_payloads",
    "lcm_gc_marks",
    "lcm_gc_meta",
    "lcm_lifecycle_state",
    "lcm_maintenance_debt",
    "lcm_raw_messages",
    "lcm_raw_messages_fts",
    "lcm_raw_messages_fts_config",
    "lcm_raw_messages_fts_data",
    "lcm_raw_messages_fts_docsize",
    "lcm_raw_messages_fts_idx",
    "lcm_raw_predecessor_ranges",
    "lcm_summary_convergence_dirty_raw",
    "lcm_summary_convergence_invalidation_work",
    "lcm_summary_convergence_queue",
    "lcm_summary_nodes",
    "lcm_summary_nodes_fts",
    "lcm_summary_nodes_fts_config",
    "lcm_summary_nodes_fts_data",
    "lcm_summary_nodes_fts_docsize",
    "lcm_summary_nodes_fts_idx",
    "lcm_summary_sources",
];

const LCM_TABLE_COLUMNS: &[(&str, &[&str])] = &[
    (
        "lcm_raw_messages",
        &[
            "provider",
            "message_id",
            "session_id",
            "store_id",
            "role",
            "ordinal",
            "timestamp",
            "content",
            "content_hash",
            "storage_kind",
            "payload_ref",
            "snippet_text",
            "index_text",
            "legacy_source",
            "legacy_truncated",
            "metadata_json",
        ],
    ),
    (
        "lcm_external_payloads",
        &[
            "payload_ref",
            "provider",
            "session_id",
            "message_id",
            "kind",
            "content_hash",
            "byte_count",
            "char_count",
            "created_at",
            "metadata_json",
        ],
    ),
    (
        "lcm_gc_marks",
        &["payload_ref", "state", "first_seen_at", "updated_at"],
    ),
    ("lcm_gc_meta", &["key", "value"]),
    (
        "lcm_summary_nodes",
        &[
            "node_id",
            "provider",
            "conversation_id",
            "session_id",
            "depth",
            "summary_text",
            "summary_hash",
            "summary_token_count",
            "source_token_count",
            "source_time_start",
            "source_time_end",
            "expand_hint",
            "metadata_json",
            "created_at",
        ],
    ),
    (
        "lcm_summary_sources",
        &["node_id", "source_kind", "source_id", "ordinal"],
    ),
    (
        "lcm_lifecycle_state",
        &[
            "provider",
            "conversation_id",
            "current_session_id",
            "last_finalized_session_id",
            "current_frontier_store_id",
            "last_finalized_frontier_store_id",
            "rollover_at",
            "reset_at",
            "maintenance_at",
            "boundary_skip_at",
            "updated_at",
        ],
    ),
    (
        "lcm_maintenance_debt",
        &[
            "provider",
            "conversation_id",
            "debt_id",
            "debt_kind",
            "from_store_id",
            "to_store_id",
            "metadata_json",
            "created_at",
        ],
    ),
];

// `pragma_table_info` catches column-order drift, while these definitions
// catch the constraints that pragma omits from the admission contract:
// defaults, CHECK clauses and foreign-key targets/actions.  SQLite preserves
// the canonical CREATE statement in sqlite_master, so compact comparison is
// stable across harmless formatting changes while remaining byte-sensitive to
// the actual SQL contract.
const LCM_TABLE_DEFINITIONS: &[(&str, &str)] = &[
    (
        "lcm_raw_messages",
        r#"
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
        )
        "#,
    ),
    (
        "lcm_external_payloads",
        r#"
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
        )
        "#,
    ),
    (
        "lcm_gc_marks",
        r#"
        CREATE TABLE lcm_gc_marks (
            payload_ref TEXT PRIMARY KEY,
            state TEXT NOT NULL CHECK(state IN ('unreferenced', 'missing')),
            first_seen_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL DEFAULT (unixepoch())
        )
        "#,
    ),
    (
        "lcm_gc_meta",
        r#"
        CREATE TABLE lcm_gc_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        "#,
    ),
    (
        "lcm_summary_nodes",
        r#"
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
        )
        "#,
    ),
    (
        "lcm_summary_sources",
        r#"
        CREATE TABLE lcm_summary_sources (
            node_id TEXT NOT NULL,
            source_kind TEXT NOT NULL CHECK(source_kind IN ('raw_message', 'summary_node')),
            source_id TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            PRIMARY KEY(node_id, ordinal),
            FOREIGN KEY(node_id) REFERENCES lcm_summary_nodes(node_id) ON DELETE CASCADE
        )
        "#,
    ),
    (
        "lcm_lifecycle_state",
        r#"
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
        )
        "#,
    ),
    (
        "lcm_maintenance_debt",
        r#"
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
        )
        "#,
    ),
];

const LCM_BETA37_INDEX_DEFINITIONS: &[(&str, &str)] = &[
    (
        "idx_lcm_external_payloads_owner",
        "CREATE INDEX idx_lcm_external_payloads_owner ON lcm_external_payloads(provider, session_id)",
    ),
    (
        "idx_lcm_maintenance_debt_kind",
        "CREATE INDEX idx_lcm_maintenance_debt_kind ON lcm_maintenance_debt(provider, debt_kind, created_at)",
    ),
    (
        "idx_lcm_raw_session_id",
        "CREATE INDEX idx_lcm_raw_session_id ON lcm_raw_messages(session_id)",
    ),
    (
        "idx_lcm_raw_session_order",
        "CREATE INDEX idx_lcm_raw_session_order ON lcm_raw_messages(provider, session_id, store_id)",
    ),
    (
        "idx_lcm_summary_nodes_codex_pending_root_order",
        r#"
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
            WHERE provider = 'codex'
        "#,
    ),
    (
        "idx_lcm_summary_nodes_codex_pending_session_order",
        r#"
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
            WHERE provider = 'codex'
        "#,
    ),
    (
        "idx_lcm_summary_nodes_session_depth_time",
        "CREATE INDEX idx_lcm_summary_nodes_session_depth_time ON lcm_summary_nodes(provider, session_id, depth, source_time_start, source_time_end, created_at)",
    ),
    (
        "idx_lcm_summary_sources_source",
        "CREATE INDEX idx_lcm_summary_sources_source ON lcm_summary_sources(source_kind, source_id)",
    ),
];

const LCM_CURRENT_INDEX_DEFINITIONS: &[(&str, &str)] = &[
    (
        "idx_lcm_external_payloads_owner_bytes",
        "CREATE INDEX idx_lcm_external_payloads_owner_bytes ON lcm_external_payloads(provider, session_id, byte_count)",
    ),
    (
        "idx_lcm_maintenance_debt_kind",
        "CREATE INDEX idx_lcm_maintenance_debt_kind ON lcm_maintenance_debt(provider, debt_kind, created_at)",
    ),
    (
        "idx_lcm_raw_direct_user_candidate",
        "CREATE INDEX idx_lcm_raw_direct_user_candidate ON lcm_raw_messages(provider, store_id) WHERE role = 'user'",
    ),
    (
        "idx_lcm_raw_legacy_truncated",
        "CREATE INDEX idx_lcm_raw_legacy_truncated ON lcm_raw_messages(provider, session_id) WHERE legacy_truncated != 0",
    ),
    (
        "idx_lcm_raw_lossy_ingest",
        "CREATE INDEX idx_lcm_raw_lossy_ingest ON lcm_raw_messages(provider, session_id) WHERE metadata_json IS NOT NULL AND json_valid(metadata_json) AND json_type(metadata_json, '$.ingest_protection.lossy') = 'true'",
    ),
    (
        "idx_lcm_raw_predecessor_session_to",
        "CREATE INDEX idx_lcm_raw_predecessor_session_to ON lcm_raw_predecessor_ranges(provider, session_id, to_store_id, message_id)",
    ),
    (
        "idx_lcm_raw_session_id",
        "CREATE INDEX idx_lcm_raw_session_id ON lcm_raw_messages(session_id)",
    ),
    (
        "idx_lcm_raw_session_order",
        "CREATE INDEX idx_lcm_raw_session_order ON lcm_raw_messages(provider, session_id, store_id)",
    ),
    (
        "idx_lcm_summary_convergence_due",
        "CREATE INDEX idx_lcm_summary_convergence_due ON lcm_summary_convergence_queue(next_attempt_at_ms, attempt_generation, queue_id) WHERE state IN ('pending', 'retryable')",
    ),
    (
        "idx_lcm_summary_nodes_codex_pending_root_order",
        LCM_BETA37_INDEX_DEFINITIONS[4].1,
    ),
    (
        "idx_lcm_summary_nodes_codex_pending_session_order",
        LCM_BETA37_INDEX_DEFINITIONS[5].1,
    ),
    (
        "idx_lcm_summary_nodes_depth_tokens",
        "CREATE INDEX idx_lcm_summary_nodes_depth_tokens ON lcm_summary_nodes(provider, session_id, depth, summary_token_count, source_token_count)",
    ),
    (
        "idx_lcm_summary_nodes_session_depth_time",
        "CREATE INDEX idx_lcm_summary_nodes_session_depth_time ON lcm_summary_nodes(provider, session_id, depth, source_time_start, source_time_end, created_at)",
    ),
    (
        "idx_lcm_summary_sources_source",
        "CREATE INDEX idx_lcm_summary_sources_source ON lcm_summary_sources(source_kind, source_id)",
    ),
    (
        "idx_lcm_summary_sources_source_node",
        "CREATE INDEX idx_lcm_summary_sources_source_node ON lcm_summary_sources(source_kind, source_id, node_id)",
    ),
];

const LCM_LEGACY_OWNER_INDEX_DEFINITION: &str =
    "CREATE INDEX idx_lcm_external_payloads_owner ON lcm_external_payloads(provider, session_id)";
const LCM_PREDECESSOR_RANGE_TABLE_DEFINITION: &str = r#"
CREATE TABLE lcm_raw_predecessor_ranges (
    provider TEXT NOT NULL,
    message_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    from_store_id INTEGER NOT NULL,
    to_store_id INTEGER NOT NULL,
    PRIMARY KEY(provider, message_id),
    FOREIGN KEY(provider, message_id)
        REFERENCES lcm_raw_messages(provider, message_id) ON DELETE CASCADE,
    FOREIGN KEY(provider, session_id)
        REFERENCES sessions(provider, session_id) ON DELETE CASCADE
)
"#;

const LCM_CURRENT_REPAIRABLE_OBJECTS: &[&str] = &[
    "idx_lcm_external_payloads_owner_bytes",
    "idx_lcm_raw_direct_user_candidate",
    "idx_lcm_raw_legacy_truncated",
    "idx_lcm_raw_lossy_ingest",
    "idx_lcm_raw_predecessor_session_to",
    "idx_lcm_summary_convergence_due",
    "idx_lcm_summary_nodes_depth_tokens",
    "idx_lcm_summary_sources_source_node",
    "lcm_raw_predecessor_ranges",
    "lcm_summary_convergence_dirty_raw",
    "lcm_summary_convergence_dirty_raw_seed",
    "lcm_summary_convergence_invalidation_work",
    "lcm_summary_convergence_queue",
    "lcm_summary_convergence_raw_insert",
    "lcm_summary_convergence_raw_unprotected_update",
];

const LCM_LEGACY_OWNER_INDEX: &str = "idx_lcm_external_payloads_owner";

/// Read-only classification of a profile store's LCM schema state.
///
/// A store whose persisted marker is not the current version, or that carries
/// LCM objects or legacy session content without a marker, requires an
/// explicit profile reset. Admission callers run this before other schema
/// authorities so the truthful LCM state is surfaced rather than masked by a
/// coarser authority's reset.
pub async fn require_admissible_lcm_schema(
    conn: &(impl QueryExecutor + ?Sized),
) -> Result<LcmSchemaAdmission, LcmError> {
    match stored_schema_version(conn).await? {
        Some(LCM_SCHEMA_VERSION) => {
            if lcm_schema_matches(conn, LCM_CURRENT_SCHEMA_OBJECTS, true).await? {
                Ok(LcmSchemaAdmission::Current)
            } else if lcm_schema_matches(conn, LCM_BETA37_SCHEMA_OBJECTS, false).await? {
                Ok(LcmSchemaAdmission::ReleasedBeta37)
            } else if lcm_schema_matches_current_with_known_gaps(conn).await? {
                Ok(LcmSchemaAdmission::Current)
            } else {
                Err(LcmError::ProfileResetRequired {
                    found_version: Some(LCM_SCHEMA_VERSION),
                    required_version: LCM_SCHEMA_VERSION,
                })
            }
        }
        Some(found_version) => Err(LcmError::ProfileResetRequired {
            found_version: Some(found_version),
            required_version: LCM_SCHEMA_VERSION,
        }),
        None if lcm_schema_objects_exist(conn).await?
            || legacy_session_content_exists(conn).await? =>
        {
            Err(LcmError::ProfileResetRequired {
                found_version: None,
                required_version: LCM_SCHEMA_VERSION,
            })
        }
        None => Ok(LcmSchemaAdmission::Fresh),
    }
}

pub async fn ensure_lcm_schema_in_transaction(
    conn: &(impl Executor + ?Sized),
) -> Result<(), LcmError> {
    match require_admissible_lcm_schema(conn).await? {
        LcmSchemaAdmission::Current => {
            ensure_raw_identity_schema(conn).await?;
            super::summary_convergence::ensure_schema(conn).await?;
            for sql in LCM_STATUS_PERFORMANCE_INDEX_SQL {
                conn.execute_batch(sql).await?;
            }
            if !lcm_schema_matches(conn, LCM_CURRENT_SCHEMA_OBJECTS, true).await? {
                return Err(LcmError::ProfileResetRequired {
                    found_version: Some(LCM_SCHEMA_VERSION),
                    required_version: LCM_SCHEMA_VERSION,
                });
            }
            return Ok(());
        }
        LcmSchemaAdmission::ReleasedBeta37 => {
            return migrate_released_beta37_lcm_schema(conn).await;
        }
        LcmSchemaAdmission::Fresh => {}
    }

    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS session_schema_migrations (
            name TEXT PRIMARY KEY,
            version INTEGER NOT NULL,
            applied_at INTEGER NOT NULL DEFAULT (unixepoch())
        );
        CREATE TABLE IF NOT EXISTS lcm_raw_messages (
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
        CREATE INDEX IF NOT EXISTS idx_lcm_raw_session_order
            ON lcm_raw_messages(provider, session_id, store_id);
        CREATE INDEX IF NOT EXISTS idx_lcm_raw_session_id
            ON lcm_raw_messages(session_id);
        CREATE TABLE IF NOT EXISTS lcm_external_payloads (
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
        CREATE TABLE IF NOT EXISTS lcm_gc_marks (
            payload_ref TEXT PRIMARY KEY,
            state TEXT NOT NULL CHECK(state IN ('unreferenced', 'missing')),
            first_seen_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL DEFAULT (unixepoch())
        );
        CREATE TABLE IF NOT EXISTS lcm_gc_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS lcm_summary_nodes (
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
        CREATE TABLE IF NOT EXISTS lcm_summary_sources (
            node_id TEXT NOT NULL,
            source_kind TEXT NOT NULL CHECK(source_kind IN ('raw_message', 'summary_node')),
            source_id TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            PRIMARY KEY(node_id, ordinal),
            FOREIGN KEY(node_id) REFERENCES lcm_summary_nodes(node_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_lcm_summary_nodes_session_depth_time
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
        CREATE INDEX IF NOT EXISTS idx_lcm_summary_sources_source
            ON lcm_summary_sources(source_kind, source_id);
        CREATE TABLE IF NOT EXISTS lcm_lifecycle_state (
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
        CREATE TABLE IF NOT EXISTS lcm_maintenance_debt (
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
        CREATE INDEX IF NOT EXISTS idx_lcm_maintenance_debt_kind
            ON lcm_maintenance_debt(provider, debt_kind, created_at);
        CREATE VIRTUAL TABLE IF NOT EXISTS lcm_summary_nodes_fts USING fts5(
            summary_text, expand_hint, metadata_json,
            content='lcm_summary_nodes',
            content_rowid='rowid'
        );
        CREATE TRIGGER IF NOT EXISTS lcm_summary_nodes_fts_insert
            AFTER INSERT ON lcm_summary_nodes BEGIN
                INSERT INTO lcm_summary_nodes_fts(rowid, summary_text, expand_hint, metadata_json)
                VALUES (NEW.rowid, NEW.summary_text, NEW.expand_hint, NEW.metadata_json);
            END;
        CREATE TRIGGER IF NOT EXISTS lcm_summary_nodes_fts_delete
            AFTER DELETE ON lcm_summary_nodes BEGIN
                INSERT INTO lcm_summary_nodes_fts(
                    lcm_summary_nodes_fts, rowid, summary_text, expand_hint, metadata_json
                )
                VALUES ('delete', OLD.rowid, OLD.summary_text, OLD.expand_hint, OLD.metadata_json);
            END;
        CREATE TRIGGER IF NOT EXISTS lcm_summary_nodes_fts_update
            AFTER UPDATE ON lcm_summary_nodes BEGIN
                INSERT INTO lcm_summary_nodes_fts(
                    lcm_summary_nodes_fts, rowid, summary_text, expand_hint, metadata_json
                )
                VALUES ('delete', OLD.rowid, OLD.summary_text, OLD.expand_hint, OLD.metadata_json);
                INSERT INTO lcm_summary_nodes_fts(rowid, summary_text, expand_hint, metadata_json)
                VALUES (NEW.rowid, NEW.summary_text, NEW.expand_hint, NEW.metadata_json);
            END;",
    )
    .await?;
    ensure_raw_identity_schema(conn).await?;
    conn.execute_batch(RAW_FTS_DDL).await?;
    super::summary_convergence::ensure_schema(conn).await?;
    super::summary_convergence::retire_predecessor_range_rewrite(conn).await?;
    for sql in LCM_STATUS_PERFORMANCE_INDEX_SQL {
        conn.execute_batch(sql).await?;
    }

    conn.execute(
        "INSERT INTO session_schema_migrations(name, version) VALUES (?1, ?2)",
        params![MIGRATION_NAME, LCM_SCHEMA_VERSION],
    )
    .await?;
    if !lcm_schema_matches(conn, LCM_CURRENT_SCHEMA_OBJECTS, true).await? {
        return Err(LcmError::ProfileResetRequired {
            found_version: Some(LCM_SCHEMA_VERSION),
            required_version: LCM_SCHEMA_VERSION,
        });
    }
    Ok(())
}

async fn ensure_raw_identity_schema(conn: &(impl Executor + ?Sized)) -> Result<(), LcmError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS lcm_raw_predecessor_ranges (
            provider TEXT NOT NULL,
            message_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            from_store_id INTEGER NOT NULL,
            to_store_id INTEGER NOT NULL,
            PRIMARY KEY(provider, message_id),
            FOREIGN KEY(provider, message_id)
                REFERENCES lcm_raw_messages(provider, message_id) ON DELETE CASCADE,
            FOREIGN KEY(provider, session_id)
                REFERENCES sessions(provider, session_id) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_lcm_raw_predecessor_session_to
            ON lcm_raw_predecessor_ranges(provider, session_id, to_store_id, message_id);
        ",
    )
    .await?;
    Ok(())
}

async fn lcm_schema_matches(
    conn: &(impl QueryExecutor + ?Sized),
    expected_objects: &[&str],
    current: bool,
) -> Result<bool, LcmError> {
    if !lcm_schema_inventory_matches(conn, expected_objects).await? {
        return Ok(false);
    }
    if !table_definitions_match(conn, LCM_TABLE_DEFINITIONS).await?
        || !table_columns_match(conn, LCM_TABLE_COLUMNS).await?
        || !index_definitions_match(
            conn,
            if current {
                LCM_CURRENT_INDEX_DEFINITIONS
            } else {
                LCM_BETA37_INDEX_DEFINITIONS
            },
            false,
        )
        .await?
        || raw_fts_structure_is_current(conn).await != Some(true)
        || !summary_fts_structure_is_current(conn).await?
    {
        return Ok(false);
    }
    if current && !super::summary_convergence::schema_contract_is_current(conn, false).await? {
        return Ok(false);
    }
    if current
        && (!sqlite_object_matches(
            conn,
            "lcm_raw_predecessor_ranges",
            "table",
            LCM_PREDECESSOR_RANGE_TABLE_DEFINITION,
        )
        .await?
            || !table_columns_match(
                conn,
                &[(
                    "lcm_raw_predecessor_ranges",
                    &[
                        "provider",
                        "message_id",
                        "session_id",
                        "from_store_id",
                        "to_store_id",
                    ],
                )],
            )
            .await?)
    {
        return Ok(false);
    }
    Ok(true)
}

/// Existing current stores can be one of the known resumable convergence
/// boundaries: the status indexes, predecessor-range authority, or summary
/// convergence objects may not have been built yet. These gaps are repaired
/// by their owning idempotent stages. Unknown objects (including objects
/// attached to an LCM table or views whose SQL references one), core column
/// drift, and malformed FTS contracts remain reset-required.
async fn lcm_schema_matches_current_with_known_gaps(
    conn: &(impl QueryExecutor + ?Sized),
) -> Result<bool, LcmError> {
    let actual = lcm_schema_inventory(conn).await?;
    let mut allowed = LCM_CURRENT_SCHEMA_OBJECTS
        .iter()
        .map(|name| (*name).to_owned())
        .collect::<BTreeSet<_>>();
    allowed.insert(LCM_LEGACY_OWNER_INDEX.to_owned());
    let actual_types = lcm_schema_inventory_types(conn).await?;
    if actual_types.iter().any(|(name, object_type)| {
        !allowed.contains(name) || expected_schema_object_type(name) != object_type
    }) {
        return Ok(false);
    }

    let required = LCM_CURRENT_SCHEMA_OBJECTS
        .iter()
        .filter(|name| !LCM_CURRENT_REPAIRABLE_OBJECTS.contains(name))
        .map(|name| (*name).to_owned())
        .collect::<BTreeSet<_>>();
    if required.iter().any(|name| !actual.contains(name)) {
        return Ok(false);
    }
    if !actual.contains("idx_lcm_external_payloads_owner_bytes")
        && !actual.contains(LCM_LEGACY_OWNER_INDEX)
    {
        return Ok(false);
    }
    if actual.contains("idx_lcm_external_payloads_owner_bytes")
        && actual.contains(LCM_LEGACY_OWNER_INDEX)
    {
        return Ok(false);
    }
    if actual.contains(LCM_LEGACY_OWNER_INDEX)
        && !sqlite_object_matches(
            conn,
            LCM_LEGACY_OWNER_INDEX,
            "index",
            LCM_LEGACY_OWNER_INDEX_DEFINITION,
        )
        .await?
    {
        return Ok(false);
    }
    if !table_definitions_match(conn, LCM_TABLE_DEFINITIONS).await?
        || !table_columns_match(conn, LCM_TABLE_COLUMNS).await?
        || !index_definitions_match(conn, LCM_CURRENT_INDEX_DEFINITIONS, true).await?
        || raw_fts_structure_is_current(conn).await != Some(true)
        || !summary_fts_structure_is_current(conn).await?
        || !super::summary_convergence::schema_contract_is_current(conn, true).await?
    {
        return Ok(false);
    }
    if actual.contains("lcm_raw_predecessor_ranges")
        && (!sqlite_object_matches(
            conn,
            "lcm_raw_predecessor_ranges",
            "table",
            LCM_PREDECESSOR_RANGE_TABLE_DEFINITION,
        )
        .await?
            || !table_columns_match(
                conn,
                &[(
                    "lcm_raw_predecessor_ranges",
                    &[
                        "provider",
                        "message_id",
                        "session_id",
                        "from_store_id",
                        "to_store_id",
                    ],
                )],
            )
            .await?)
    {
        return Ok(false);
    }
    Ok(true)
}

async fn lcm_schema_inventory(
    conn: &(impl QueryExecutor + ?Sized),
) -> Result<BTreeSet<String>, LcmError> {
    Ok(lcm_schema_inventory_types(conn)
        .await?
        .into_keys()
        .collect())
}

async fn lcm_schema_inventory_types(
    conn: &(impl QueryExecutor + ?Sized),
) -> Result<BTreeMap<String, String>, LcmError> {
    let mut rows = conn
        .query(
            "SELECT name, type, tbl_name, COALESCE(sql, '')
             FROM sqlite_master
             WHERE name NOT LIKE 'sqlite\\_%' ESCAPE '\\'
             ORDER BY name",
            (),
        )
        .await?;
    let mut objects = BTreeMap::new();
    while let Some(row) = rows.next().await? {
        let name = row.get::<String>(0)?;
        let object_type = row.get::<String>(1)?;
        let table_name = row.get::<String>(2)?;
        let sql = row.get::<String>(3)?;
        if is_lcm_schema_object(&object_type, &name, &table_name, &sql) {
            objects.insert(name, object_type);
        }
    }
    Ok(objects)
}

fn is_lcm_schema_object(object_type: &str, name: &str, table_name: &str, sql: &str) -> bool {
    [name, table_name]
        .into_iter()
        .any(|value| is_lcm_schema_object_name(value))
        || (object_type.eq_ignore_ascii_case("view") && sql_mentions_lcm_table_identifier(sql))
}

fn is_lcm_schema_object_name(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name.starts_with("lcm_") || name.starts_with("idx_lcm_")
}

fn sql_mentions_lcm_table_identifier(sql: &str) -> bool {
    LCM_TABLE_REFERENCE_NAMES
        .iter()
        .any(|table| sql_mentions_identifier(sql, table))
}

fn sql_mentions_identifier(sql: &str, identifier: &str) -> bool {
    let identifier = identifier.to_ascii_lowercase();
    sql.to_ascii_lowercase()
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .any(|token| token == identifier)
}

async fn lcm_schema_inventory_matches(
    conn: &(impl QueryExecutor + ?Sized),
    expected_objects: &[&str],
) -> Result<bool, LcmError> {
    let actual = lcm_schema_inventory_types(conn).await?;
    let expected = expected_objects
        .iter()
        .map(|name| {
            (
                (*name).to_owned(),
                expected_schema_object_type(name).to_owned(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    Ok(actual == expected)
}

fn expected_schema_object_type(name: &str) -> &'static str {
    if name.starts_with("idx_lcm_") {
        "index"
    } else if matches!(
        name,
        "lcm_raw_messages_fts_insert"
            | "lcm_raw_messages_fts_delete"
            | "lcm_raw_messages_fts_update"
            | "lcm_summary_nodes_fts_insert"
            | "lcm_summary_nodes_fts_delete"
            | "lcm_summary_nodes_fts_update"
            | "lcm_summary_convergence_dirty_raw_seed"
            | "lcm_summary_convergence_raw_insert"
            | "lcm_summary_convergence_raw_unprotected_update"
    ) {
        "trigger"
    } else {
        "table"
    }
}

async fn table_columns_match(
    conn: &(impl QueryExecutor + ?Sized),
    contracts: &[(&str, &[&str])],
) -> Result<bool, LcmError> {
    for (table, expected) in contracts {
        let mut rows = conn
            .query(
                "SELECT name FROM pragma_table_info(?1) ORDER BY cid",
                params![*table],
            )
            .await?;
        let mut actual = Vec::with_capacity(expected.len());
        while let Some(row) = rows.next().await? {
            actual.push(row.get::<String>(0)?);
        }
        if actual
            .iter()
            .map(String::as_str)
            .ne(expected.iter().copied())
        {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn table_definitions_match(
    conn: &(impl QueryExecutor + ?Sized),
    definitions: &[(&str, &str)],
) -> Result<bool, LcmError> {
    for (name, expected_sql) in definitions {
        if !sqlite_object_matches(conn, name, "table", expected_sql).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn index_definitions_match(
    conn: &(impl QueryExecutor + ?Sized),
    definitions: &[(&str, &str)],
    allow_missing: bool,
) -> Result<bool, LcmError> {
    for (name, expected_sql) in definitions {
        let Some((object_type, actual_sql)) = sqlite_object_definition(conn, name).await? else {
            if allow_missing {
                continue;
            }
            return Ok(false);
        };
        if object_type != "index" || compact_sql(&actual_sql) != compact_sql(expected_sql) {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn sqlite_object_definition(
    conn: &(impl QueryExecutor + ?Sized),
    name: &str,
) -> Result<Option<(String, String)>, LcmError> {
    let mut rows = conn
        .query(
            "SELECT type, COALESCE(sql, '') FROM sqlite_master WHERE name = ?1",
            params![name],
        )
        .await?;
    rows.next()
        .await?
        .map(|row| Ok((row.get(0)?, row.get(1)?)))
        .transpose()
}

async fn sqlite_object_matches(
    conn: &(impl QueryExecutor + ?Sized),
    name: &str,
    object_type: &str,
    expected_sql: &str,
) -> Result<bool, LcmError> {
    let Some((actual_type, actual_sql)) = sqlite_object_definition(conn, name).await? else {
        return Ok(false);
    };
    Ok(actual_type == object_type && compact_sql(&actual_sql) == compact_sql(expected_sql))
}

async fn summary_fts_structure_is_current(
    conn: &(impl QueryExecutor + ?Sized),
) -> Result<bool, LcmError> {
    for (name, object_type, expected_sql) in SUMMARY_FTS_OBJECT_DEFINITIONS {
        if !sqlite_object_matches(conn, name, object_type, expected_sql).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

async fn migrate_released_beta37_lcm_schema(
    conn: &(impl Executor + ?Sized),
) -> Result<(), LcmError> {
    ensure_raw_identity_schema(conn).await?;
    super::summary_convergence::ensure_schema(conn).await?;
    for sql in LCM_STATUS_PERFORMANCE_INDEX_SQL {
        conn.execute_batch(sql).await?;
    }
    let updated = conn
        .execute(
            "UPDATE session_schema_migrations
             SET applied_at = unixepoch()
             WHERE name = ?1 AND version = ?2",
            params![MIGRATION_NAME, LCM_SCHEMA_VERSION],
        )
        .await?;
    if updated != 1 {
        return Err(LcmError::ProfileResetRequired {
            found_version: Some(LCM_SCHEMA_VERSION),
            required_version: LCM_SCHEMA_VERSION,
        });
    }
    if !lcm_schema_matches(conn, LCM_CURRENT_SCHEMA_OBJECTS, true).await? {
        return Err(LcmError::ProfileResetRequired {
            found_version: Some(LCM_SCHEMA_VERSION),
            required_version: LCM_SCHEMA_VERSION,
        });
    }
    Ok(())
}

pub async fn schema_version(conn: &(impl QueryExecutor + ?Sized)) -> Option<i64> {
    stored_schema_version(conn).await.ok().flatten()
}

async fn stored_schema_version(
    conn: &(impl QueryExecutor + ?Sized),
) -> Result<Option<i64>, LcmError> {
    if !schema_object_exists(conn, "session_schema_migrations").await? {
        return Ok(None);
    }
    let mut rows = conn
        .query(
            "SELECT version FROM session_schema_migrations WHERE name = ?1",
            params![MIGRATION_NAME],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some(row.get(0)?)),
        None => Ok(None),
    }
}

async fn lcm_schema_objects_exist(conn: &(impl QueryExecutor + ?Sized)) -> Result<bool, LcmError> {
    let mut rows = conn
        .query(
            "SELECT type, name, tbl_name, COALESCE(sql, '')
             FROM sqlite_master
             WHERE name NOT LIKE 'sqlite\\_%' ESCAPE '\\'",
            (),
        )
        .await?;
    while let Some(row) = rows.next().await? {
        let object_type = row.get::<String>(0)?;
        let name = row.get::<String>(1)?;
        let table_name = row.get::<String>(2)?;
        let sql = row.get::<String>(3)?;
        if is_lcm_schema_object(&object_type, &name, &table_name, &sql) {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn legacy_session_content_exists(
    conn: &(impl QueryExecutor + ?Sized),
) -> Result<bool, LcmError> {
    if !schema_object_exists(conn, "session_messages").await? {
        return Ok(false);
    }
    let mut rows = conn
        .query("SELECT EXISTS(SELECT 1 FROM session_messages)", ())
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| LcmError::Db("legacy session content probe returned no rows".to_owned()))?;
    Ok(row.get::<i64>(0)? != 0)
}

async fn schema_object_exists(
    conn: &(impl QueryExecutor + ?Sized),
    name: &str,
) -> Result<bool, LcmError> {
    let mut rows = conn
        .query(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name = ?1)",
            params![name],
        )
        .await?;
    let row = rows
        .next()
        .await?
        .ok_or_else(|| LcmError::Db("schema object probe returned no rows".to_owned()))?;
    Ok(row.get::<i64>(0)? != 0)
}

pub async fn get_gc_meta(
    conn: &(impl QueryExecutor + ?Sized),
    key: &str,
) -> Result<Option<String>, LcmError> {
    let mut rows = conn
        .query("SELECT value FROM lcm_gc_meta WHERE key = ?1", params![key])
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some(row.get(0)?)),
        None => Ok(None),
    }
}

pub async fn set_gc_meta(
    conn: &(impl Executor + ?Sized),
    key: &str,
    value: &str,
) -> Result<(), LcmError> {
    conn.execute(
        "INSERT OR REPLACE INTO lcm_gc_meta (key, value) VALUES (?1, ?2)",
        params![key, value],
    )
    .await?;
    Ok(())
}

pub async fn clear_gc_meta(conn: &(impl Executor + ?Sized), key: &str) -> Result<(), LcmError> {
    conn.execute("DELETE FROM lcm_gc_meta WHERE key = ?1", params![key])
        .await?;
    Ok(())
}

pub async fn load_raw_message(
    conn: &(impl QueryExecutor + ?Sized),
    provider: &str,
    message_id: &str,
) -> Result<Option<LcmRawMessage>, LcmError> {
    let sql = format!(
        "SELECT {}
         FROM lcm_raw_messages
         WHERE provider = ?1 AND message_id = ?2
         ORDER BY store_id
         LIMIT 2",
        raw::RAW_MESSAGE_SELECT_COLUMNS
    );
    let mut rows = conn.query(&sql, params![provider, message_id]).await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let message = raw::verified_raw_message_from_row(&row)?;
    if rows.next().await?.is_some() {
        return Err(LcmError::Db(
            "duplicate raw messages for provider/message identity".to_string(),
        ));
    }
    Ok(Some(message))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracedecay_privacy::sanitize_lcm_payload_text;
    use tracedecay_runtime_core::db::engine::TestConnection;

    const RELEASED_BETA37_FIXTURE: &str = include_str!("../tests/fixtures/lcm-released-beta37.sql");

    async fn load_released_beta37_fixture(
        conn: &TestConnection,
        fixture: &str,
    ) -> Result<(), String> {
        conn.execute_batch(fixture)
            .await
            .map_err(|error| error.to_string())
    }

    /// Snapshot logical persisted rows, including FTS content indexes and all
    /// durable convergence work. FTS shadow tables are implementation detail
    /// and are rebuilt as part of SQLite's virtual-table lifecycle; content,
    /// queue, invalidation, and predecessor-range rows are the durable
    /// contract migration must preserve.
    async fn fixture_data_snapshot(conn: &TestConnection) -> Result<Vec<(String, String)>, String> {
        let tables = [
            (
                "sessions",
                "provider, session_id, project_key, project_path",
            ),
            (
                "session_messages",
                "provider, message_id, session_id, role, timestamp, ordinal, text, metadata_json",
            ),
            (
                "lcm_raw_messages",
                "provider, message_id, session_id, store_id, role, ordinal, timestamp, content, content_hash, storage_kind, payload_ref, snippet_text, index_text, legacy_source, legacy_truncated, metadata_json",
            ),
            (
                "lcm_external_payloads",
                "payload_ref, provider, session_id, message_id, kind, content_hash, byte_count, char_count, created_at, metadata_json",
            ),
            (
                "lcm_gc_marks",
                "payload_ref, state, first_seen_at, updated_at",
            ),
            ("lcm_gc_meta", "key, value"),
            (
                "lcm_lifecycle_state",
                "provider, conversation_id, current_session_id, last_finalized_session_id, current_frontier_store_id, last_finalized_frontier_store_id, rollover_at, reset_at, maintenance_at, boundary_skip_at, updated_at",
            ),
            (
                "lcm_maintenance_debt",
                "provider, conversation_id, debt_id, debt_kind, from_store_id, to_store_id, metadata_json, created_at",
            ),
            (
                "lcm_summary_nodes",
                "node_id, provider, conversation_id, session_id, depth, summary_text, summary_hash, summary_token_count, source_token_count, source_time_start, source_time_end, expand_hint, metadata_json, created_at",
            ),
            (
                "lcm_summary_sources",
                "node_id, source_kind, source_id, ordinal",
            ),
            ("lcm_raw_messages_fts", "rowid, index_text"),
            (
                "lcm_summary_nodes_fts",
                "rowid, summary_text, expand_hint, metadata_json",
            ),
            (
                "lcm_summary_convergence_queue",
                "queue_id, provider, session_id, newest_raw_store_id, protection_frontier_store_id, attempted_raw_store_id, state, failure_code, failure_count, next_attempt_at_ms, attempt_generation, raw_revision_generation, stale_from_store_id",
            ),
            (
                "lcm_summary_convergence_dirty_raw",
                "provider, session_id, store_id, rewind_frontier_store_id",
            ),
            (
                "lcm_summary_convergence_invalidation_work",
                "provider, session_id, raw_store_id, source_kind, source_id, depth, after_node_id, state",
            ),
            (
                "lcm_raw_predecessor_ranges",
                "provider, message_id, session_id, from_store_id, to_store_id",
            ),
        ];
        let mut snapshot = Vec::new();
        for (table, columns) in tables {
            if matches!(
                table,
                "lcm_summary_convergence_queue"
                    | "lcm_summary_convergence_dirty_raw"
                    | "lcm_summary_convergence_invalidation_work"
                    | "lcm_raw_predecessor_ranges"
            ) && !schema_object_exists(&*conn, table)
                .await
                .map_err(|error| error.to_string())?
            {
                continue;
            }
            let column_count = columns.split(", ").count();
            let row_column_count = i32::try_from(column_count)
                .map_err(|error| format!("snapshot column count overflow: {error}"))?;
            let quoted_columns = columns
                .split(", ")
                .map(|column| format!("quote({column})"))
                .collect::<Vec<_>>()
                .join(", ");
            let order = (1..=column_count)
                .map(|column| column.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!("SELECT {quoted_columns} FROM {table} ORDER BY {order}");
            let mut rows = conn
                .query(&sql, ())
                .await
                .map_err(|error| error.to_string())?;
            while let Some(row) = rows.next().await.map_err(|error| error.to_string())? {
                let mut values = Vec::new();
                for column in 0..row_column_count {
                    values.push(
                        row.get::<String>(column)
                            .map_err(|error| error.to_string())?,
                    );
                }
                snapshot.push((table.to_string(), values.join("|")));
            }
        }
        Ok(snapshot)
    }

    async fn lcm_reader_test_connection() -> Result<(tempfile::TempDir, TestConnection), String> {
        let temp = tempfile::tempdir().map_err(|error| error.to_string())?;
        let conn = TestConnection::open(&temp.path().join("sessions.db"));
        conn.execute_batch(
            "CREATE TABLE lcm_raw_messages (
                provider TEXT NOT NULL,
                message_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                store_id INTEGER PRIMARY KEY AUTOINCREMENT,
                role TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                timestamp INTEGER,
                content TEXT,
                content_hash TEXT NOT NULL,
                storage_kind TEXT NOT NULL,
                payload_ref TEXT,
                snippet_text TEXT NOT NULL,
                index_text TEXT NOT NULL,
                legacy_source INTEGER NOT NULL DEFAULT 0,
                legacy_truncated INTEGER NOT NULL DEFAULT 0,
                metadata_json TEXT,
                UNIQUE(provider, message_id)
            );",
        )
        .await
        .map_err(|error| error.to_string())?;
        Ok((temp, conn))
    }

    async fn insert_reader_test_message(
        conn: &TestConnection,
        content: &str,
        storage_kind: &str,
        metadata_json: Option<&str>,
    ) -> Result<(), String> {
        let content_hash = projected_content_hash(content);
        conn.execute(
            "INSERT INTO lcm_raw_messages (
                provider, message_id, session_id, role, ordinal, content,
                content_hash, storage_kind, snippet_text, index_text, metadata_json
             ) VALUES (
                'cursor', 'message-1', 'session-1', 'user', 1, ?1,
                ?2, ?3, ?1, ?1, ?4
             )",
            params![content, content_hash, storage_kind, metadata_json],
        )
        .await
        .map_err(|error| error.to_string())?;
        Ok(())
    }

    #[tokio::test]
    async fn raw_fts_currency_requires_table_and_every_trigger_contract() -> Result<(), String> {
        let temp = tempfile::tempdir().map_err(|error| error.to_string())?;
        let conn = TestConnection::open(&temp.path().join("sessions.db"));
        conn.execute_batch(
            "CREATE TABLE lcm_raw_messages (
                store_id INTEGER PRIMARY KEY,
                index_text TEXT NOT NULL
            );",
        )
        .await
        .map_err(|error| error.to_string())?;
        rebuild_raw_fts(&*conn)
            .await
            .ok_or_else(|| "initial raw FTS rebuild failed".to_string())?;
        assert_eq!(raw_fts_structure_is_current(&*conn).await, Some(true));

        for trigger in [
            "lcm_raw_messages_fts_insert",
            "lcm_raw_messages_fts_delete",
            "lcm_raw_messages_fts_update",
        ] {
            conn.execute_batch(&format!("DROP TRIGGER {trigger}"))
                .await
                .map_err(|error| error.to_string())?;
            assert_eq!(
                raw_fts_structure_is_current(&*conn).await,
                Some(false),
                "missing {trigger} was accepted as current"
            );
            rebuild_raw_fts(&*conn)
                .await
                .ok_or_else(|| format!("raw FTS rebuild failed after dropping {trigger}"))?;
            assert_eq!(raw_fts_structure_is_current(&*conn).await, Some(true));
        }

        conn.execute_batch(
            "DROP TRIGGER lcm_raw_messages_fts_update;
             CREATE TRIGGER lcm_raw_messages_fts_update
                 AFTER UPDATE ON lcm_raw_messages BEGIN
                     SELECT 1;
                 END;",
        )
        .await
        .map_err(|error| error.to_string())?;
        assert_eq!(
            raw_fts_structure_is_current(&*conn).await,
            Some(false),
            "malformed update trigger was accepted as current"
        );

        conn.execute_batch("DROP TABLE lcm_raw_messages_fts")
            .await
            .map_err(|error| error.to_string())?;
        assert_eq!(
            raw_fts_structure_is_current(&*conn).await,
            Some(false),
            "missing raw FTS table was accepted as current"
        );
        Ok(())
    }

    #[tokio::test]
    async fn released_beta37_fixture_converges_losslessly_and_idempotently() -> Result<(), String> {
        let temp = tempfile::tempdir().map_err(|error| error.to_string())?;
        let conn = TestConnection::open(&temp.path().join("sessions.db"));
        load_released_beta37_fixture(&conn, RELEASED_BETA37_FIXTURE).await?;

        assert_eq!(
            require_admissible_lcm_schema(&*conn)
                .await
                .map_err(|error| error.to_string())?,
            LcmSchemaAdmission::ReleasedBeta37
        );
        let data_before = fixture_data_snapshot(&conn).await?;

        ensure_lcm_schema(&conn)
            .await
            .map_err(|error| error.to_string())?;
        assert_eq!(schema_version(&*conn).await, Some(LCM_SCHEMA_VERSION));
        assert_eq!(
            require_admissible_lcm_schema(&*conn)
                .await
                .map_err(|error| error.to_string())?,
            LcmSchemaAdmission::Current
        );
        assert_eq!(fixture_data_snapshot(&conn).await?, data_before);

        // The migration adds the current work tables, but queue and range
        // rows are populated by their bounded convergence stages.  Running
        // those stages here proves the released rows remain queryable while
        // retry, invalidation and predecessor-range state is materialized.
        let backfill = crate::summary_convergence::backfill_queue_page(&*conn, 128)
            .await
            .map_err(|error| error.to_string())?;
        assert_eq!(backfill.rows_scanned, 4);
        assert!(!backfill.has_more);
        assert_eq!(
            util::fetch_i64(
                &*conn,
                "SELECT COUNT(*) FROM lcm_summary_convergence_queue",
                (),
                "queue rows",
            )
            .await
            .map_err(|error| error.to_string())?,
            2
        );
        assert_eq!(
            util::fetch_i64(
                &*conn,
                "SELECT COUNT(*) FROM lcm_raw_predecessor_ranges",
                (),
                "predecessor ranges",
            )
            .await
            .map_err(|error| error.to_string())?,
            2
        );

        // Seed the remaining durable convergence rows before the idempotent
        // reopen. The snapshot below must observe every field on these rows;
        // dropping or rewriting queue, dirty, invalidation, or range state
        // would then fail this migration regression.
        conn.execute(
            "UPDATE lcm_raw_messages
             SET metadata_json = '{\"fixture_revision\":true}'
             WHERE provider = 'cursor' AND message_id = 'beta37-message-a1'",
            (),
        )
        .await
        .map_err(|error| error.to_string())?;
        conn.execute(
            "UPDATE lcm_summary_convergence_invalidation_work
             SET after_node_id = 'summary-beta37-root', state = 'drained'
             WHERE provider = 'cursor' AND session_id = 'beta37-session-a'
               AND raw_store_id = 10 AND source_kind = 'raw_message'",
            (),
        )
        .await
        .map_err(|error| error.to_string())?;
        assert_eq!(
            util::fetch_i64(
                &*conn,
                "SELECT COUNT(*) FROM lcm_summary_convergence_dirty_raw
                 WHERE provider = 'cursor' AND session_id = 'beta37-session-a'",
                (),
                "dirty raw rows",
            )
            .await
            .map_err(|error| error.to_string())?,
            1
        );
        assert_eq!(
            util::fetch_i64(
                &*conn,
                "SELECT COUNT(*) FROM lcm_summary_convergence_invalidation_work
                 WHERE provider = 'cursor' AND session_id = 'beta37-session-a'",
                (),
                "invalidation work rows",
            )
            .await
            .map_err(|error| error.to_string())?,
            1
        );

        let schema_after_migration = sqlite_schema_fingerprint(&conn).await?;
        let data_after_migration = fixture_data_snapshot(&conn).await?;
        ensure_lcm_schema(&conn)
            .await
            .map_err(|error| error.to_string())?;
        assert_eq!(
            sqlite_schema_fingerprint(&conn).await?,
            schema_after_migration
        );
        assert_eq!(fixture_data_snapshot(&conn).await?, data_after_migration);
        Ok(())
    }

    #[tokio::test]
    async fn released_beta37_fixture_rejects_arbitrary_view_referencing_lcm_table()
    -> Result<(), String> {
        let temp = tempfile::tempdir().map_err(|error| error.to_string())?;
        let conn = TestConnection::open(&temp.path().join("sessions.db"));
        load_released_beta37_fixture(&conn, RELEASED_BETA37_FIXTURE).await?;
        conn.execute_batch(
            "CREATE VIEW arbitrary_reference AS
             SELECT provider, message_id FROM lcm_raw_messages;",
        )
        .await
        .map_err(|error| error.to_string())?;
        let schema_before = sqlite_schema_fingerprint(&conn).await?;
        let data_before = fixture_data_snapshot(&conn).await?;

        let error = ensure_lcm_schema(&conn)
            .await
            .expect_err("an arbitrary view over LCM data must require reset");
        assert!(matches!(error, LcmError::ProfileResetRequired { .. }));
        assert_eq!(sqlite_schema_fingerprint(&conn).await?, schema_before);
        assert_eq!(fixture_data_snapshot(&conn).await?, data_before);
        Ok(())
    }

    async fn assert_released_beta37_drift_refused_without_mutation(
        fixture: &str,
        drift: &str,
    ) -> Result<(), String> {
        let temp = tempfile::tempdir().map_err(|error| error.to_string())?;
        let conn = TestConnection::open(&temp.path().join("sessions.db"));
        load_released_beta37_fixture(&conn, fixture).await?;
        if !drift.is_empty() {
            conn.execute_batch(drift)
                .await
                .map_err(|error| error.to_string())?;
        }
        let schema_before = sqlite_schema_fingerprint(&conn).await?;
        let data_before = fixture_data_snapshot(&conn).await?;

        let error = ensure_lcm_schema(&conn)
            .await
            .expect_err("schema drift must require an explicit profile reset");
        assert!(matches!(error, LcmError::ProfileResetRequired { .. }));
        assert_eq!(sqlite_schema_fingerprint(&conn).await?, schema_before);
        assert_eq!(fixture_data_snapshot(&conn).await?, data_before);
        Ok(())
    }

    #[tokio::test]
    async fn released_beta37_fixture_rejects_constraint_index_and_inert_trigger_drift()
    -> Result<(), String> {
        assert_released_beta37_drift_refused_without_mutation(
            RELEASED_BETA37_FIXTURE,
            "CREATE INDEX unexpected_beta37 ON lcm_raw_messages(provider);",
        )
        .await?;
        assert_released_beta37_drift_refused_without_mutation(
            RELEASED_BETA37_FIXTURE,
            "DROP INDEX idx_lcm_summary_sources_source;
             CREATE INDEX idx_lcm_summary_sources_source
                 ON lcm_summary_sources(node_id);",
        )
        .await?;
        assert_released_beta37_drift_refused_without_mutation(
            RELEASED_BETA37_FIXTURE,
            "DROP TRIGGER lcm_raw_messages_fts_insert;
             CREATE TRIGGER lcm_raw_messages_fts_insert
                 AFTER INSERT ON lcm_raw_messages WHEN 0 BEGIN
                     SELECT 1;
                 END;",
        )
        .await?;
        assert_released_beta37_drift_refused_without_mutation(
            RELEASED_BETA37_FIXTURE,
            "DROP TRIGGER lcm_summary_nodes_fts_update;
             CREATE TRIGGER lcm_summary_nodes_fts_update
                 AFTER UPDATE ON lcm_summary_nodes WHEN 0 BEGIN
                     SELECT 1;
                 END;",
        )
        .await?;

        let malformed_constraint = RELEASED_BETA37_FIXTURE.replace(
            "state TEXT NOT NULL CHECK(state IN ('unreferenced', 'missing'))",
            "state TEXT NOT NULL",
        );
        assert_ne!(malformed_constraint, RELEASED_BETA37_FIXTURE);
        assert_released_beta37_drift_refused_without_mutation(&malformed_constraint, "").await?;
        Ok(())
    }

    #[tokio::test]
    async fn released_beta37_migration_rolls_back_all_mutations_on_marker_failure()
    -> Result<(), String> {
        let temp = tempfile::tempdir().map_err(|error| error.to_string())?;
        let conn = TestConnection::open(&temp.path().join("sessions.db"));
        load_released_beta37_fixture(&conn, RELEASED_BETA37_FIXTURE).await?;
        conn.execute_batch(
            "CREATE TRIGGER fail_lcm_marker
                 BEFORE UPDATE OF applied_at ON session_schema_migrations
                 WHEN OLD.name = 'lcm'
                 BEGIN
                     SELECT RAISE(ABORT, 'test migration failure');
                 END;",
        )
        .await
        .map_err(|error| error.to_string())?;
        let schema_before = sqlite_schema_fingerprint(&conn).await?;
        let data_before = fixture_data_snapshot(&conn).await?;

        let error = ensure_lcm_schema(&conn)
            .await
            .expect_err("marker failure must roll back the released migration");
        assert!(matches!(error, LcmError::Db(_)));
        assert_eq!(sqlite_schema_fingerprint(&conn).await?, schema_before);
        assert_eq!(fixture_data_snapshot(&conn).await?, data_before);
        assert_eq!(schema_version(&*conn).await, Some(LCM_SCHEMA_VERSION));

        conn.execute_batch("DROP TRIGGER fail_lcm_marker")
            .await
            .map_err(|error| error.to_string())?;
        ensure_lcm_schema(&conn)
            .await
            .map_err(|error| error.to_string())?;
        assert_eq!(
            require_admissible_lcm_schema(&*conn)
                .await
                .map_err(|error| error.to_string())?,
            LcmSchemaAdmission::Current
        );
        Ok(())
    }

    #[tokio::test]
    async fn incompatible_profile_requires_reset_without_mutating_schema() -> Result<(), String> {
        let temp = tempfile::tempdir().map_err(|error| error.to_string())?;
        let conn = TestConnection::open(&temp.path().join("sessions.db"));
        conn.execute_batch(
            "CREATE TABLE sessions (
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
            INSERT INTO session_schema_migrations(name, version, applied_at)
            VALUES ('lcm', 6, 123);
            CREATE VIRTUAL TABLE lcm_lifecycle_state USING fts5(
                provider,
                conversation_id,
                current_session_id
            );",
        )
        .await
        .map_err(|error| error.to_string())?;

        let schema_before = sqlite_schema_fingerprint(&conn).await?;
        let error = ensure_lcm_schema(&conn)
            .await
            .expect_err("an incompatible profile must require an explicit reset");
        assert!(matches!(
            error,
            LcmError::ProfileResetRequired {
                found_version: Some(6),
                required_version: LCM_SCHEMA_VERSION,
            }
        ));
        assert_eq!(sqlite_schema_fingerprint(&conn).await?, schema_before);
        assert_eq!(
            util::fetch_i64(
                &*conn,
                "SELECT version FROM session_schema_migrations WHERE name = 'lcm'",
                (),
                "migration marker version",
            )
            .await
            .map_err(|error| error.to_string())?,
            6
        );
        assert_eq!(
            util::fetch_i64(
                &*conn,
                "SELECT COUNT(*) FROM pragma_table_xinfo('lcm_lifecycle_state')
                 WHERE name = 'boundary_skip_at'",
                (),
                "boundary column count",
            )
            .await
            .map_err(|error| error.to_string())?,
            0
        );
        Ok(())
    }

    #[tokio::test]
    async fn fresh_schema_never_carries_forward_legacy_session_content() -> Result<(), String> {
        let temp = tempfile::tempdir().map_err(|error| error.to_string())?;
        let conn = TestConnection::open(&temp.path().join("sessions.db"));
        conn.execute_batch(
            "CREATE TABLE sessions (
                provider TEXT NOT NULL,
                session_id TEXT NOT NULL,
                project_key TEXT NOT NULL,
                project_path TEXT NOT NULL,
                title TEXT,
                started_at INTEGER,
                ended_at INTEGER,
                transcript_path TEXT,
                metadata_json TEXT,
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
                kind TEXT,
                model TEXT,
                tool_names TEXT,
                source_path TEXT,
                source_offset INTEGER,
                metadata_json TEXT,
                PRIMARY KEY(provider, message_id)
            );
            CREATE TABLE session_schema_migrations (
                name TEXT PRIMARY KEY,
                version INTEGER NOT NULL,
                applied_at INTEGER NOT NULL DEFAULT (unixepoch())
            );",
        )
        .await
        .map_err(|err| err.to_string())?;

        ensure_lcm_schema(&conn)
            .await
            .map_err(|error| error.to_string())?;
        conn.execute_batch(
            "INSERT INTO sessions(provider, session_id, project_key, project_path)
             VALUES ('cursor', 'legacy-session', '/tmp/project', '/tmp/project');
             INSERT INTO session_messages(provider, message_id, session_id, role, ordinal, text)
             VALUES
               ('cursor', 'legacy-message-1', 'legacy-session', 'assistant', 1, 'legacy one'),
               ('cursor', 'legacy-message-2', 'legacy-session', 'assistant', 2, 'legacy two');",
        )
        .await
        .map_err(|error| error.to_string())?;
        ensure_lcm_schema(&conn)
            .await
            .map_err(|error| error.to_string())?;
        assert_eq!(
            util::fetch_i64(
                &*conn,
                "SELECT COUNT(*) FROM lcm_raw_messages",
                (),
                "raw count",
            )
            .await
            .map_err(|err| err.to_string())?,
            0
        );
        assert_eq!(
            util::fetch_i64(
                &*conn,
                "SELECT version FROM session_schema_migrations WHERE name = 'lcm'",
                (),
                "migration marker version",
            )
            .await
            .map_err(|err| err.to_string())?,
            LCM_SCHEMA_VERSION
        );
        assert_eq!(
            util::fetch_i64(
                &*conn,
                "SELECT COUNT(*) FROM session_messages",
                (),
                "legacy message count",
            )
            .await
            .map_err(|err| err.to_string())?,
            2
        );
        Ok(())
    }

    /// A fresh install carries every status performance index, and the
    /// superseded plain payload owner index is gone — its replacement covers
    /// the same leading columns.
    #[tokio::test]
    async fn fresh_schema_installs_status_performance_indexes() -> Result<(), String> {
        let temp = tempfile::tempdir().map_err(|error| error.to_string())?;
        let conn = TestConnection::open(&temp.path().join("sessions.db"));
        conn.execute_batch(
            "CREATE TABLE sessions (
                provider TEXT NOT NULL,
                session_id TEXT NOT NULL,
                project_key TEXT NOT NULL,
                project_path TEXT NOT NULL,
                PRIMARY KEY(provider, session_id)
            );",
        )
        .await
        .map_err(|error| error.to_string())?;
        ensure_lcm_schema(&conn)
            .await
            .map_err(|error| error.to_string())?;

        for index in [
            "idx_lcm_raw_legacy_truncated",
            "idx_lcm_raw_lossy_ingest",
            "idx_lcm_summary_nodes_depth_tokens",
            "idx_lcm_external_payloads_owner_bytes",
        ] {
            assert!(
                schema_object_exists(&*conn, index)
                    .await
                    .map_err(|error| error.to_string())?,
                "fresh LCM schema is missing status performance index {index}"
            );
        }
        assert!(
            !schema_object_exists(&*conn, "idx_lcm_external_payloads_owner")
                .await
                .map_err(|error| error.to_string())?,
            "the superseded plain payload owner index must not be reinstalled"
        );
        assert!(
            schema_object_exists(&*conn, "lcm_raw_predecessor_ranges")
                .await
                .map_err(|error| error.to_string())?,
            "fresh LCM schema is missing native source-range provenance"
        );
        assert!(
            schema_object_exists(&*conn, "idx_lcm_raw_predecessor_session_to")
                .await
                .map_err(|error| error.to_string())?,
            "fresh LCM schema is missing the bounded native-range lookup index"
        );
        conn.execute_batch("DROP TABLE lcm_raw_predecessor_ranges;")
            .await
            .map_err(|error| error.to_string())?;
        ensure_lcm_schema(&conn)
            .await
            .map_err(|error| error.to_string())?;
        assert!(
            schema_object_exists(&*conn, "lcm_raw_predecessor_ranges")
                .await
                .map_err(|error| error.to_string())?,
            "current LCM schema did not repair native source-range provenance"
        );
        assert!(
            schema_object_exists(&*conn, "idx_lcm_raw_predecessor_session_to")
                .await
                .map_err(|error| error.to_string())?,
            "current LCM schema did not repair the bounded native-range lookup index"
        );
        Ok(())
    }

    #[tokio::test]
    async fn raw_message_load_propagates_poisoned_storage_kind() -> Result<(), String> {
        let (_temp, conn) = lcm_reader_test_connection().await?;
        insert_reader_test_message(&conn, "safe content", "poisoned", None).await?;

        let error = load_raw_message(&*conn, "cursor", "message-1")
            .await
            .expect_err("poisoned storage kind must not collapse to absence");

        assert!(matches!(error, LcmError::Db(message) if message.contains("invalid storage_kind")));
        Ok(())
    }

    #[tokio::test]
    async fn raw_message_load_rejects_mismatched_sanitization_receipt() -> Result<(), String> {
        let (_temp, conn) = lcm_reader_test_connection().await?;
        let sanitization = sanitize_lcm_payload_text("receipt-bound content")
            .map_err(|error| error.to_string())?;
        let metadata = serde_json::json!({
            "ingest_protection": {
                "sanitization_receipt": sanitization.receipt()
            }
        })
        .to_string();
        insert_reader_test_message(&conn, "tampered content", "inline", Some(&metadata)).await?;

        let error = load_raw_message(&*conn, "cursor", "message-1")
            .await
            .expect_err("receipt mismatch must not return a raw row");

        assert_eq!(error, LcmError::PayloadIntegrityMismatch);
        Ok(())
    }

    #[tokio::test]
    async fn raw_message_load_propagates_database_failure() -> Result<(), String> {
        let (_temp, conn) = lcm_reader_test_connection().await?;
        conn.execute_batch("DROP TABLE lcm_raw_messages")
            .await
            .map_err(|error| error.to_string())?;

        let error = load_raw_message(&*conn, "cursor", "message-1")
            .await
            .expect_err("database failure must not collapse to absence");

        assert!(matches!(error, LcmError::Db(_)));
        Ok(())
    }

    #[tokio::test]
    async fn fresh_admission_rejects_orphan_lcm_index_on_host_table() -> Result<(), String> {
        let temp = tempfile::tempdir().map_err(|error| error.to_string())?;
        let conn = TestConnection::open(&temp.path().join("sessions.db"));
        conn.execute_batch(
            "CREATE TABLE sessions (
                provider TEXT NOT NULL,
                session_id TEXT NOT NULL,
                project_key TEXT NOT NULL,
                project_path TEXT NOT NULL,
                PRIMARY KEY(provider, session_id)
            );
            CREATE INDEX idx_lcm_stray ON sessions(project_key);",
        )
        .await
        .map_err(|error| error.to_string())?;
        let schema_before = sqlite_schema_fingerprint(&conn).await?;

        let error = ensure_lcm_schema(&conn)
            .await
            .expect_err("an orphan LCM-named host index must require reset");
        assert!(matches!(error, LcmError::ProfileResetRequired { .. }));
        assert_eq!(sqlite_schema_fingerprint(&conn).await?, schema_before);
        Ok(())
    }

    #[tokio::test]
    async fn fresh_admission_rejects_arbitrary_lcm_reference_view() -> Result<(), String> {
        let temp = tempfile::tempdir().map_err(|error| error.to_string())?;
        let conn = TestConnection::open(&temp.path().join("sessions.db"));
        conn.execute_batch(
            "CREATE VIEW arbitrary_reference AS
             SELECT 1 FROM lcm_raw_messages;",
        )
        .await
        .map_err(|error| error.to_string())?;
        let schema_before = sqlite_schema_fingerprint(&conn).await?;

        let error = require_admissible_lcm_schema(&*conn)
            .await
            .expect_err("an arbitrary view over an LCM table must require reset");
        assert!(matches!(error, LcmError::ProfileResetRequired { .. }));
        assert_eq!(sqlite_schema_fingerprint(&conn).await?, schema_before);
        Ok(())
    }

    async fn sqlite_schema_fingerprint(
        conn: &Connection,
    ) -> Result<Vec<(String, String, String)>, String> {
        let mut rows = conn
            .query(
                "SELECT type, name, COALESCE(sql, '')
                 FROM sqlite_master
                 WHERE name NOT LIKE 'sqlite_%'
                   AND (
                       name = 'session_schema_migrations'
                    OR name LIKE 'lcm_%'
                    OR name LIKE 'idx_lcm_%'
                    OR tbl_name LIKE 'lcm_%'
                   )
                 ORDER BY type, name",
                (),
            )
            .await
            .map_err(|error| error.to_string())?;
        let mut fingerprint = Vec::new();
        while let Some(row) = rows.next().await.map_err(|error| error.to_string())? {
            fingerprint.push((
                row.get(0).map_err(|error| error.to_string())?,
                row.get(1).map_err(|error| error.to_string())?,
                row.get(2).map_err(|error| error.to_string())?,
            ));
        }
        Ok(fingerprint)
    }
}
