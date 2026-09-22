//! The one in-place upgrade path for project stores written by beta.37.
//!
//! The beta.37 binary stamped its project store `user_version = 34`.  The
//! current binary creates the v36 shape, so a released store needs a
//! deliberately bounded bridge.  This module is intentionally separate from
//! the ordinary exact-shape admission path: the source inventory is checked
//! against the released fixture before any write is issued, and the whole
//! bridge runs in one caller-owned transaction.

use std::{collections::BTreeMap, sync::LazyLock};

use crate::db::engine::{Executor, QueryExecutor, params};
use crate::db::{evidence_assembly, external_source, memory_v2};
use tracedecay_domain::errors::{Result, TraceDecayError};

const OPERATION: &str = "migrate released project schema";
const RELEASED_V34_STAMP: u32 = 34;
const RELEASED_V35_STAMP: u32 = 35;

/// The complete v35 project-store DDL snapshot. This is an immutable
/// admission boundary: changes to the current schema installers must never
/// change which historical store is accepted as v35.
const RELEASED_V35_PROJECT_STORE_SQL: &str =
    include_str!("../../../tests/fixtures/project-store-released-v35-semantic.sql");

/// v35 was the last pre-v36 shape. A few stores can also carry the shipped
/// v35 alias trigger, whose broad update guard is admitted as a separate
/// exact inventory and repaired as part of the same transaction.
const SHIPPED_V35_ALIAS_UPDATE_TRIGGER: &str = "
    CREATE TRIGGER retrieval_anchor_aliases_immutable_update
    BEFORE UPDATE ON retrieval_anchor_aliases BEGIN
        SELECT RAISE(ABORT, 'retrieval anchor aliases are immutable');
    END;
";

/// The payload-digest objects that were introduced by the known v35 step.
/// Keeping this copy next to the source-shape fixture makes the accepted v35
/// inventory explicit.  The current memory schema installs the same DDL.
const PAYLOAD_DIGESTS_SCHEMA: &str =
    "CREATE TABLE IF NOT EXISTS memory_v2_assertion_payload_digests (
            payload_rowid INTEGER PRIMARY KEY,
            assertion_id TEXT NOT NULL,
            fact_id TEXT NOT NULL,
            owner_kind TEXT NOT NULL,
            project_id TEXT NOT NULL,
            content_digest TEXT NOT NULL CHECK(
                length(content_digest) = 71 AND content_digest LIKE 'sha256:%'
            ),
            UNIQUE(assertion_id, fact_id, owner_kind, project_id),
            FOREIGN KEY(payload_rowid)
                REFERENCES memory_v2_assertion_payloads(rowid)
        );

        CREATE INDEX IF NOT EXISTS memory_v2_assertion_payload_digests_lookup
            ON memory_v2_assertion_payload_digests(
                owner_kind, project_id, content_digest, fact_id
            );

        CREATE TRIGGER IF NOT EXISTS memory_v2_assertion_payload_digests_no_update
        BEFORE UPDATE ON memory_v2_assertion_payload_digests BEGIN
            SELECT RAISE(ABORT, 'memory_v2 assertion payload digests are immutable');
        END;
        CREATE TRIGGER IF NOT EXISTS memory_v2_payloads_digest_delete
        AFTER DELETE ON memory_v2_assertion_payloads BEGIN
            DELETE FROM memory_v2_assertion_payload_digests
            WHERE payload_rowid = OLD.rowid;
        END;";

/// The semantic-vector staging family was retired in v36. It is a derived
/// projection with no lossless representation in the final store, so a
/// released store carrying rows there is unsupported and must be reset rather
/// than silently deleting durable-looking work.
const RETIRED_SEMANTIC_TABLES: &[&str] = &[
    "semantic_vector_stage_chunk_receipts",
    "semantic_vector_stage_graph_effects",
    "semantic_vector_stage_batches",
    "semantic_vector_source_scope_bindings",
    "semantic_vector_stage_adoption_authority",
    "semantic_vector_stage_census_authority",
    "semantic_vector_retirement_cleanup",
    "semantic_vector_stages",
];

/// The fixture is assembled from the tagged beta.37 DDL, rather than from
/// this binary's current schema.  Embedding it in the migration keeps source
/// admission independent from any future final-shape changes.
const RELEASED_V34_PROJECT_STORE_SQL: &str =
    include_str!("../../../tests/fixtures/project-store-released-v34.sql");

#[derive(Clone, Debug, PartialEq, Eq)]
struct SchemaObject {
    object_type: String,
    table: String,
    sql: String,
}

type SchemaInventory = BTreeMap<String, SchemaObject>;

struct ReleasedShapes {
    v34: SchemaInventory,
    v35_legacy: SchemaInventory,
    v35: SchemaInventory,
    v35_shipped_alias: SchemaInventory,
}

static RELEASED_SHAPES: LazyLock<std::result::Result<ReleasedShapes, String>> =
    LazyLock::new(build_released_shapes);

fn build_released_shapes() -> std::result::Result<ReleasedShapes, String> {
    let connection = rusqlite::Connection::open_in_memory()
        .map_err(|error| format!("failed to open released-shape fixture: {error}"))?;
    connection
        .execute_batch(RELEASED_V34_PROJECT_STORE_SQL)
        .map_err(|error| format!("failed to install released-v34 fixture: {error}"))?;
    let v34 = read_rusqlite_inventory(&connection)?;
    connection
        .execute_batch(PAYLOAD_DIGESTS_SCHEMA)
        .map_err(|error| format!("failed to install released-v35 payload objects: {error}"))?;
    let v35_legacy = read_rusqlite_inventory(&connection)?;

    let canonical = rusqlite::Connection::open_in_memory()
        .map_err(|error| format!("failed to open canonical released-v35 fixture: {error}"))?;
    canonical
        .execute_batch(RELEASED_V35_PROJECT_STORE_SQL)
        .map_err(|error| format!("failed to install released-v35 fixture: {error}"))?;
    let v35 = read_rusqlite_inventory(&canonical)?;
    canonical
        .execute_batch(
            "DROP TRIGGER retrieval_anchor_aliases_immutable_update;
             ",
        )
        .map_err(|error| format!("failed to prepare shipped-v35 trigger fixture: {error}"))?;
    canonical
        .execute_batch(SHIPPED_V35_ALIAS_UPDATE_TRIGGER)
        .map_err(|error| format!("failed to install shipped-v35 trigger fixture: {error}"))?;
    let v35_shipped_alias = read_rusqlite_inventory(&canonical)?;

    Ok(ReleasedShapes {
        v34,
        v35_legacy,
        v35,
        v35_shipped_alias,
    })
}

fn failure(message: impl Into<String>) -> TraceDecayError {
    TraceDecayError::Database {
        message: message.into(),
        operation: OPERATION.to_owned(),
    }
}

fn reset_required(message: impl Into<String>) -> TraceDecayError {
    TraceDecayError::reset_required(
        "SQLite store",
        format!(
            "{}; run `tracedecay storage reset-project-store` with this store's \
             `--project-root` or `--project-id`, then let this binary create the exact final shape",
            message.into()
        ),
    )
}

fn read_rusqlite_inventory(
    connection: &rusqlite::Connection,
) -> std::result::Result<SchemaInventory, String> {
    let mut statement = connection
        .prepare(
            "SELECT type, name, tbl_name, COALESCE(sql, '')
             FROM sqlite_master
             WHERE type IN ('table', 'index', 'trigger', 'view')
               AND name NOT LIKE 'sqlite_%'
             ORDER BY name",
        )
        .map_err(|error| format!("failed to prepare released schema inventory: {error}"))?;
    let rows = statement
        .query_map((), |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })
        .map_err(|error| format!("failed to query released schema inventory: {error}"))?;
    let mut inventory = SchemaInventory::new();
    for row in rows {
        let (object_type, name, table, sql) =
            row.map_err(|error| format!("failed to read released schema object: {error}"))?;
        if inventory
            .insert(
                name.clone(),
                SchemaObject {
                    object_type,
                    table,
                    sql,
                },
            )
            .is_some()
        {
            return Err(format!("released schema repeats object '{name}'"));
        }
    }
    Ok(inventory)
}

async fn read_inventory(conn: &impl QueryExecutor) -> Result<SchemaInventory> {
    let mut rows = conn
        .query(
            "SELECT type, name, tbl_name, COALESCE(sql, '')
             FROM sqlite_master
             WHERE type IN ('table', 'index', 'trigger', 'view')
               AND name NOT LIKE 'sqlite_%'
             ORDER BY name",
            (),
        )
        .await
        .map_err(|error| failure(format!("failed to query source schema inventory: {error}")))?;
    let mut inventory = SchemaInventory::new();
    while let Some(row) = rows
        .next()
        .await
        .map_err(|error| failure(format!("failed to read source schema inventory: {error}")))?
    {
        let object_type = row
            .get::<String>(0)
            .map_err(|error| failure(format!("failed to decode source object type: {error}")))?;
        let name = row
            .get::<String>(1)
            .map_err(|error| failure(format!("failed to decode source object name: {error}")))?;
        let table = row
            .get::<String>(2)
            .map_err(|error| failure(format!("failed to decode source object table: {error}")))?;
        let sql = row
            .get::<String>(3)
            .map_err(|error| failure(format!("failed to decode source object SQL: {error}")))?;
        if inventory
            .insert(
                name.clone(),
                SchemaObject {
                    object_type,
                    table,
                    sql,
                },
            )
            .is_some()
        {
            return Err(failure(format!("source schema repeats object '{name}'")));
        }
    }
    Ok(inventory)
}

fn shape_difference(actual: &SchemaInventory, expected: &SchemaInventory) -> Option<String> {
    for (name, expected_object) in expected {
        let Some(actual_object) = actual.get(name) else {
            return Some(format!(
                "source schema is missing released {} '{name}'",
                expected_object.object_type
            ));
        };
        if actual_object != expected_object {
            return Some(format!(
                "source schema has incompatible released {} '{name}'",
                expected_object.object_type
            ));
        }
    }
    actual
        .iter()
        .find(|(name, _)| !expected.contains_key(*name))
        .map(|(name, object)| {
            format!(
                "source schema contains unexpected {} '{name}'",
                object.object_type
            )
        })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SourceShape {
    V34,
    V35Legacy,
    V35LegacyWithPayloadDigests,
    V35Current,
    V35CurrentShippedAlias,
}

impl SourceShape {
    fn has_payload_digests(self) -> bool {
        matches!(
            self,
            Self::V35LegacyWithPayloadDigests | Self::V35Current | Self::V35CurrentShippedAlias
        )
    }

    fn uses_legacy_external_copies(self) -> bool {
        matches!(
            self,
            Self::V34 | Self::V35Legacy | Self::V35LegacyWithPayloadDigests
        )
    }
}

/// Checks the exact source inventory before any migration write.
/// Each accepted inventory maps to a bounded conversion branch; no unknown
/// drift is repaired by guessing which release produced it.
pub(super) async fn require_exact_source_shape(
    conn: &impl QueryExecutor,
    stamp: u32,
) -> Result<SourceShape> {
    let actual = read_inventory(conn).await?;
    let shapes = RELEASED_SHAPES
        .as_ref()
        .map_err(|error| failure(format!("failed to build released source shape: {error}")))?;
    if stamp == RELEASED_V34_STAMP && actual == shapes.v34 {
        return Ok(SourceShape::V34);
    }
    if stamp == RELEASED_V35_STAMP {
        if actual == shapes.v35 {
            return Ok(SourceShape::V35Current);
        }
        if actual == shapes.v35_shipped_alias {
            return Ok(SourceShape::V35CurrentShippedAlias);
        }
        // A v35 stamp can be left behind before its first payload object is
        // created.  It is still the same known released inventory, and the
        // bridge fills the objects atomically below.
        if actual == shapes.v35_legacy {
            return Ok(SourceShape::V35LegacyWithPayloadDigests);
        }
        if actual == shapes.v34 {
            return Ok(SourceShape::V35Legacy);
        }
    }
    let reason = if stamp != RELEASED_V34_STAMP && stamp != RELEASED_V35_STAMP {
        format!("schema stamp v{stamp} is not a supported released project-store stamp")
    } else if stamp == RELEASED_V34_STAMP {
        shape_difference(&actual, &shapes.v34)
            .unwrap_or_else(|| "source schema does not match the released v34 inventory".into())
    } else {
        shape_difference(&actual, &shapes.v35)
            .or_else(|| shape_difference(&actual, &shapes.v35_shipped_alias))
            .or_else(|| shape_difference(&actual, &shapes.v35_legacy))
            .or_else(|| shape_difference(&actual, &shapes.v34))
            .unwrap_or_else(|| "source schema does not match a released inventory".into())
    };
    Err(reset_required(reason))
}

/// Runs the released-to-v36 conversion. The caller owns the transaction and
/// rolls it back if any step fails; this function never commits or changes the
/// schema stamp until the exact final inventory has passed.
pub(super) async fn migrate_released_project_schema(
    conn: &(impl Executor + Sync),
    stamp: u32,
) -> Result<()> {
    let source_shape = require_exact_source_shape(conn, stamp).await?;
    validate_retired_semantic_projection(conn).await?;
    if source_shape.has_payload_digests() {
        validate_payload_digest_rows(conn).await?;
    }
    if source_shape.uses_legacy_external_copies() {
        validate_source_rows(conn).await?;
    }

    execute_batch(conn, "PRAGMA defer_foreign_keys = ON;").await?;
    if source_shape.uses_legacy_external_copies() {
        snapshot_diagnostics(conn).await?;
    }
    if source_shape.uses_legacy_external_copies() {
        execute_batch(
            conn,
            "DROP TRIGGER IF EXISTS retrieval_anchor_aliases_immutable_update;
             DROP TRIGGER IF EXISTS semantic_vector_replay_stage_identity_guard;
             DROP TABLE generation_diagnostics;
             DROP TABLE diagnostic_generation_publications;",
        )
        .await?;
    } else {
        execute_batch(
            conn,
            "DROP TRIGGER IF EXISTS retrieval_anchor_aliases_immutable_update;
             DROP TRIGGER IF EXISTS semantic_vector_replay_stage_identity_guard;",
        )
        .await?;
    }

    crate::db::retrieval_anchor_schema::install_retrieval_anchor_schema(conn, OPERATION).await?;
    memory_v2::create_schema(conn, OPERATION).await?;
    evidence_assembly::install_evidence_assembly_schema(conn, OPERATION).await?;
    external_source::install_external_source_schema(conn, OPERATION).await?;
    execute_batch(conn, tracedecay_store::GENERATION_DIAGNOSTICS_SCHEMA_DDL).await?;
    execute_batch(
        conn,
        tracedecay_rusqlite_runtime::repository::GRAPH_PUBLICATION_SCHEMA_V1,
    )
    .await?;
    execute_batch(
        conn,
        tracedecay_rusqlite_runtime::handoff::HANDOFF_OPEN_SCHEMA_V1,
    )
    .await?;
    execute_batch(
        conn,
        tracedecay_rusqlite_runtime::runtime_ledger::RUNTIME_LEDGER_SCHEMA,
    )
    .await?;

    if source_shape.uses_legacy_external_copies() {
        restore_diagnostics(conn).await?;
    }
    migrate_payload_digests(conn).await?;
    if source_shape.uses_legacy_external_copies() {
        migrate_external_source(conn).await?;
        migrate_runtime_ledger(conn).await?;
    }
    drop_retired_semantic_projection(conn).await?;

    super::final_shape::require_exact_final_shape(conn).await?;
    super::set_version(conn, super::SCHEMA_VERSION).await
}

/// Refuses rows in the v35 semantic staging projection before any schema or
/// data mutation. The v36 final shape intentionally has no semantic staging
/// tables, and no migration can reconstruct their provider/model payloads.
async fn validate_retired_semantic_projection(conn: &impl QueryExecutor) -> Result<()> {
    for &table in RETIRED_SEMANTIC_TABLES {
        let mut rows = conn
            .query(&format!("SELECT 1 FROM {table} LIMIT 1"), ())
            .await
            .map_err(|error| {
                failure(format!(
                    "failed to inspect retired semantic table {table}: {error}"
                ))
            })?;
        if rows
            .next()
            .await
            .map_err(|error| {
                failure(format!(
                    "failed to inspect retired semantic table {table}: {error}"
                ))
            })?
            .is_some()
        {
            return Err(reset_required(format!(
                "released project store contains rows in retired semantic table '{table}'"
            )));
        }

        // AUTOINCREMENT keeps a high-water mark even after all rows are
        // deleted. That mark is part of the released table's durable identity
        // allocation, and dropping the retired table would otherwise rewind
        // it silently. There is no lossless v36 home for it, so refuse the
        // store before issuing any migration write.
        let mut sequence_rows = conn
            .query(
                "SELECT seq FROM sqlite_sequence WHERE name = ?1",
                params![table],
            )
            .await
            .map_err(|error| {
                failure(format!(
                    "failed to inspect retired semantic sequence {table}: {error}"
                ))
            })?;
        if sequence_rows
            .next()
            .await
            .map_err(|error| {
                failure(format!(
                    "failed to inspect retired semantic sequence {table}: {error}"
                ))
            })?
            .is_some()
        {
            return Err(reset_required(format!(
                "released project store contains a retired semantic table '{table}' with a used AUTOINCREMENT sequence"
            )));
        }
    }
    Ok(())
}

async fn execute_batch(conn: &impl Executor, sql: &str) -> Result<()> {
    conn.execute_batch(sql)
        .await
        .map_err(|error| failure(format!("failed to execute migration SQL: {error}")))
}

async fn snapshot_diagnostics(conn: &impl Executor) -> Result<()> {
    execute_batch(
        conn,
        "CREATE TEMP TABLE td_migration_diagnostic_publications AS
         SELECT generation_id, record_state, state_generation, published_at
         FROM diagnostic_generation_publications;
         CREATE TEMP TABLE td_migration_generation_diagnostics AS
         SELECT diagnostic_anchor, generation_id, repository, worktree, reference,
                source_revision, file_occurrence_id, content_digest,
                symbol_occurrence_id, span_start, span_end, code, severity, message,
                message_digest, producer_kind, producer, analyzer_revision,
                configuration_revision, sanitization_receipt, evidence_class,
                collected_at, record_state, state_generation, persisted_at
         FROM generation_diagnostics;",
    )
    .await
}

async fn restore_diagnostics(conn: &impl Executor) -> Result<()> {
    conn.execute(
        "INSERT INTO diagnostic_generation_publications(
             generation_id, publication_revision, record_state, state_generation, published_at
         )
         SELECT generation_id, 1, record_state, state_generation, published_at
         FROM td_migration_diagnostic_publications",
        (),
    )
    .await
    .map_err(|error| {
        failure(format!(
            "failed to restore diagnostic publications: {error}"
        ))
    })?;
    conn.execute(
        "INSERT INTO generation_diagnostics(
             diagnostic_anchor, generation_id, publication_revision, repository, worktree,
             reference, source_revision, file_occurrence_id, content_digest,
             symbol_occurrence_id, span_start, span_end, code, severity, message,
             message_digest, producer_kind, producer, analyzer_revision,
             configuration_revision, sanitization_receipt, evidence_class,
             collected_at, record_state, state_generation, persisted_at
         )
         SELECT diagnostic_anchor, generation_id, 1, repository, worktree,
                reference, source_revision, file_occurrence_id, content_digest,
                symbol_occurrence_id, span_start, span_end, code, severity, message,
                message_digest, producer_kind, producer, analyzer_revision,
                configuration_revision, sanitization_receipt, evidence_class,
                collected_at, record_state, state_generation, persisted_at
         FROM td_migration_generation_diagnostics",
        (),
    )
    .await
    .map_err(|error| failure(format!("failed to restore generation diagnostics: {error}")))?;
    execute_batch(
        conn,
        "DROP TABLE td_migration_diagnostic_publications;
         DROP TABLE td_migration_generation_diagnostics;",
    )
    .await
}

fn payload_content_digest(content: &str) -> String {
    use sha2::Digest as _;
    tracedecay_domain::canonical_text::encode_tagged_lowercase_hex(
        "sha256:",
        &sha2::Sha256::digest(content.as_bytes()),
    )
}

async fn validate_payload_digest_rows(conn: &impl QueryExecutor) -> Result<()> {
    let mut rows = conn
        .query(
            "SELECT digests.payload_rowid, digests.assertion_id, digests.fact_id,
                    digests.owner_kind, digests.project_id, digests.content_digest,
                    payloads.content
             FROM memory_v2_assertion_payload_digests AS digests
             LEFT JOIN memory_v2_assertion_payloads AS payloads
               ON payloads.rowid = digests.payload_rowid
             ORDER BY digests.payload_rowid",
            (),
        )
        .await
        .map_err(|error| failure(format!("failed to read existing payload digests: {error}")))?;
    while let Some(row) = rows.next().await.map_err(|error| {
        failure(format!(
            "failed to read an existing payload digest: {error}"
        ))
    })? {
        let rowid = row
            .get::<i64>(0)
            .map_err(|error| failure(format!("failed to decode payload digest rowid: {error}")))?;
        let content = row
            .get::<Option<String>>(6)
            .map_err(|error| failure(format!("failed to decode payload {rowid}: {error}")))?
            .ok_or_else(|| failure(format!("payload digest {rowid} has no source payload")))?;
        let expected = payload_content_digest(&content);
        let actual = row.get::<String>(5).map_err(|error| {
            failure(format!("failed to decode payload digest {rowid}: {error}"))
        })?;
        if actual != expected {
            return Err(failure(format!(
                "payload digest {rowid} does not match its immutable payload"
            )));
        }
        for (index, label) in [
            (1, "assertion_id"),
            (2, "fact_id"),
            (3, "owner_kind"),
            (4, "project_id"),
        ] {
            let stored = row.get::<String>(index).map_err(|error| {
                failure(format!(
                    "failed to decode payload digest {rowid} {label}: {error}"
                ))
            })?;
            let source = match index {
                1 => "assertion_id",
                2 => "fact_id",
                3 => "owner_kind",
                _ => "project_id",
            };
            let mut source_rows = conn
                .query(
                    &format!("SELECT {source} FROM memory_v2_assertion_payloads WHERE rowid = ?1"),
                    params![rowid],
                )
                .await
                .map_err(|error| {
                    failure(format!("failed to verify payload digest {rowid}: {error}"))
                })?;
            let Some(source_row) = source_rows.next().await.map_err(|error| {
                failure(format!(
                    "failed to read payload digest source {rowid}: {error}"
                ))
            })?
            else {
                return Err(failure(format!(
                    "payload digest {rowid} has no source payload"
                )));
            };
            let source_value = source_row.get::<String>(0).map_err(|error| {
                failure(format!(
                    "failed to decode payload digest source {rowid}: {error}"
                ))
            })?;
            if stored != source_value {
                return Err(failure(format!(
                    "payload digest {rowid} disagrees with its source {label}"
                )));
            }
        }
    }
    Ok(())
}

async fn migrate_payload_digests(conn: &(impl Executor + Sync)) -> Result<()> {
    let mut rows = conn
        .query(
            "SELECT payloads.rowid, payloads.assertion_id, payloads.fact_id,
                    payloads.owner_kind, payloads.project_id, payloads.content
             FROM memory_v2_assertion_payloads AS payloads
             LEFT JOIN memory_v2_assertion_payload_digests AS digests
               ON digests.payload_rowid = payloads.rowid
             WHERE digests.payload_rowid IS NULL
             ORDER BY payloads.rowid",
            (),
        )
        .await
        .map_err(|error| {
            failure(format!(
                "failed to read payloads for digest migration: {error}"
            ))
        })?;
    while let Some(row) = rows.next().await.map_err(|error| {
        failure(format!(
            "failed to read a payload for digest migration: {error}"
        ))
    })? {
        let rowid = row
            .get::<i64>(0)
            .map_err(|error| failure(format!("failed to decode payload rowid: {error}")))?;
        let assertion_id = row.get::<String>(1).map_err(|error| {
            failure(format!(
                "failed to decode payload {rowid} assertion id: {error}"
            ))
        })?;
        let fact_id = row.get::<String>(2).map_err(|error| {
            failure(format!("failed to decode payload {rowid} fact id: {error}"))
        })?;
        let owner_kind = row.get::<String>(3).map_err(|error| {
            failure(format!(
                "failed to decode payload {rowid} owner kind: {error}"
            ))
        })?;
        let project_id = row.get::<String>(4).map_err(|error| {
            failure(format!(
                "failed to decode payload {rowid} project id: {error}"
            ))
        })?;
        let content = row.get::<String>(5).map_err(|error| {
            failure(format!("failed to decode payload {rowid} content: {error}"))
        })?;
        conn.execute(
            "INSERT INTO memory_v2_assertion_payload_digests(
                 payload_rowid, assertion_id, fact_id, owner_kind, project_id, content_digest
             ) VALUES(?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                rowid,
                assertion_id,
                fact_id,
                owner_kind,
                project_id,
                payload_content_digest(&content),
            ],
        )
        .await
        .map_err(|error| failure(format!("failed to write payload digest {rowid}: {error}")))?;
    }
    Ok(())
}

fn nonempty_digest(value: Option<&serde_json::Value>) -> bool {
    value
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| !value.is_empty())
}

fn valid_mutation_json(raw: &str, expected_digest: Option<&str>) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(raw) else {
        return false;
    };
    let Some(digest) = value
        .get("mutation_digest")
        .and_then(serde_json::Value::as_str)
    else {
        return false;
    };
    !digest.is_empty() && expected_digest.is_none_or(|expected_digest| digest == expected_digest)
}

#[derive(Clone, Debug)]
struct ReceiptColumns {
    idempotency_key: Option<String>,
    request_digest: Option<String>,
    definition_revision: Option<i64>,
    definition_digest: Option<String>,
    binding_revision: Option<i64>,
    binding_digest: Option<String>,
    projection_digest: Option<String>,
    source_receipt_digest: Option<String>,
    predecessor_frontier_digest: String,
    successor_frontier_digest: String,
    receipt_digest: String,
}

fn object_string_matches(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    expected: Option<&str>,
) -> bool {
    expected.is_none_or(|expected| {
        object
            .get(key)
            .and_then(serde_json::Value::as_str)
            .is_some_and(|actual| actual == expected)
    })
}

fn object_integer_matches(
    object: &serde_json::Map<String, serde_json::Value>,
    key: &str,
    expected: Option<i64>,
) -> bool {
    expected.is_none_or(|expected| {
        object
            .get(key)
            .and_then(serde_json::Value::as_i64)
            .is_some_and(|actual| actual == expected)
    })
}

fn valid_receipt_json(raw: &str, projection: bool, columns: &ReceiptColumns) -> bool {
    if columns.receipt_digest.is_empty()
        || columns.predecessor_frontier_digest.is_empty()
        || columns.successor_frontier_digest.is_empty()
        || columns
            .idempotency_key
            .as_deref()
            .is_some_and(str::is_empty)
        || columns.request_digest.as_deref().is_some_and(str::is_empty)
        || columns
            .definition_digest
            .as_deref()
            .is_some_and(str::is_empty)
        || columns.binding_digest.as_deref().is_some_and(str::is_empty)
        || columns
            .source_receipt_digest
            .as_deref()
            .is_some_and(str::is_empty)
        || columns
            .projection_digest
            .as_deref()
            .is_some_and(str::is_empty)
        || columns
            .definition_revision
            .is_some_and(|revision| revision <= 0)
        || columns
            .binding_revision
            .is_some_and(|revision| revision <= 0)
    {
        return false;
    }
    let Ok(value) = serde_json::from_str::<serde_json::Value>(raw) else {
        return false;
    };
    let Some(object) = value.as_object() else {
        return false;
    };
    if !object_string_matches(
        object,
        "idempotency_key",
        columns.idempotency_key.as_deref(),
    ) || !object_string_matches(object, "request_digest", columns.request_digest.as_deref())
        || !object_integer_matches(object, "definition_revision", columns.definition_revision)
        || !object_string_matches(
            object,
            "definition_digest",
            columns.definition_digest.as_deref(),
        )
        || !object_integer_matches(object, "binding_revision", columns.binding_revision)
        || !object_string_matches(object, "binding_digest", columns.binding_digest.as_deref())
        || !object_string_matches(object, "receipt_digest", Some(&columns.receipt_digest))
        || !object_string_matches(
            object,
            "source_receipt_digest",
            columns.source_receipt_digest.as_deref(),
        )
    {
        return false;
    }
    if let Some(projection_digest) = columns.projection_digest.as_deref()
        && !object_string_matches(object, "receipt_digest", Some(projection_digest))
    {
        return false;
    }

    let Some(source_frontier) = object.get("source_frontier") else {
        return false;
    };
    if !nonempty_digest(source_frontier.get("digest"))
        || source_frontier
            .get("digest")
            .and_then(serde_json::Value::as_str)
            != Some(columns.successor_frontier_digest.as_str())
    {
        return false;
    }

    let frontier_key = if projection {
        "expected_projection_frontier"
    } else {
        "prior_source_frontier"
    };
    let Some(prior) = object.get(frontier_key) else {
        return false;
    };
    let prior_digest = if prior.is_null() {
        Some("root")
    } else {
        if columns.predecessor_frontier_digest == "root" {
            return false;
        }
        if !nonempty_digest(prior.get("digest")) {
            return false;
        }
        prior.get("digest").and_then(serde_json::Value::as_str)
    };
    if prior_digest != Some(columns.predecessor_frontier_digest.as_str()) {
        return false;
    }

    if projection {
        // `receipt_digest` is the projection key in the v1 publication table;
        // `source_receipt_digest` is the commit receipt it hydrates from.
        if columns.source_receipt_digest.is_none() {
            return false;
        }
    }

    object
        .get("mutations")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|mutations| {
            mutations.iter().all(|mutation| {
                mutation
                    .as_object()
                    .is_some_and(|mutation| nonempty_digest(mutation.get("mutation_digest")))
            })
        })
}

fn valid_authority_receipt_json(
    raw: &str,
    idempotency_key: &str,
    request_digest: &str,
    definition_digest: &str,
    binding_digest: &str,
) -> bool {
    if idempotency_key.is_empty()
        || request_digest.is_empty()
        || definition_digest.is_empty()
        || binding_digest.is_empty()
    {
        return false;
    }
    let Ok(value) = serde_json::from_str::<serde_json::Value>(raw) else {
        return false;
    };
    let Some(object) = value.as_object() else {
        return false;
    };
    object_string_matches(object, "idempotency_key", Some(idempotency_key))
        && object_string_matches(object, "request_digest", Some(request_digest))
        && object_string_matches(object, "definition_digest", Some(definition_digest))
        && object_string_matches(object, "binding_digest", Some(binding_digest))
        && object
            .get("prior_definition_digest")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|digest| !digest.is_empty())
        && object
            .get("prior_binding_digest")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|digest| !digest.is_empty())
}

async fn validate_authority_rows(conn: &impl QueryExecutor) -> Result<()> {
    let mut rows = conn
        .query(
            "SELECT rowid, binding_id, idempotency_key, request_digest,
                    definition_digest, binding_digest, receipt_json
             FROM external_source_authority_receipts_v1 ORDER BY rowid",
            (),
        )
        .await
        .map_err(|error| failure(format!("failed to read authority receipts: {error}")))?;
    while let Some(row) = rows
        .next()
        .await
        .map_err(|error| failure(format!("failed to read authority receipt row: {error}")))?
    {
        let rowid = row
            .get::<i64>(0)
            .map_err(|error| failure(format!("failed to decode authority receipt row: {error}")))?;
        let binding_id = row.get::<String>(1).map_err(|error| {
            failure(format!(
                "failed to decode authority receipt row {rowid}: {error}"
            ))
        })?;
        let idempotency_key = row.get::<String>(2).map_err(|error| {
            failure(format!(
                "failed to decode authority receipt row {rowid}: {error}"
            ))
        })?;
        let request_digest = row.get::<String>(3).map_err(|error| {
            failure(format!(
                "failed to decode authority receipt row {rowid}: {error}"
            ))
        })?;
        let definition_digest = row.get::<String>(4).map_err(|error| {
            failure(format!(
                "failed to decode authority receipt row {rowid}: {error}"
            ))
        })?;
        let binding_digest = row.get::<String>(5).map_err(|error| {
            failure(format!(
                "failed to decode authority receipt row {rowid}: {error}"
            ))
        })?;
        let receipt_json = row.get::<String>(6).map_err(|error| {
            failure(format!(
                "failed to decode authority receipt row {rowid}: {error}"
            ))
        })?;
        if !valid_authority_receipt_json(
            &receipt_json,
            &idempotency_key,
            &request_digest,
            &definition_digest,
            &binding_digest,
        ) {
            return Err(failure(format!(
                "authority receipt row {rowid} for binding {binding_id} disagrees with its JSON"
            )));
        }
        for (label, digest, sql) in [
            (
                "definition",
                definition_digest.as_str(),
                "SELECT 1 FROM external_source_definition_revisions_v1
                 WHERE definition_digest = ?1 LIMIT 1",
            ),
            (
                "binding",
                binding_digest.as_str(),
                "SELECT 1 FROM external_source_binding_revisions_v1
                 WHERE binding_digest = ?1 LIMIT 1",
            ),
        ] {
            let mut history = conn.query(sql, params![digest]).await.map_err(|error| {
                failure(format!(
                    "failed to read authority {label} history for row {rowid}: {error}"
                ))
            })?;
            if history
                .next()
                .await
                .map_err(|error| {
                    failure(format!(
                        "failed to read authority {label} history for row {rowid}: {error}"
                    ))
                })?
                .is_none()
            {
                return Err(failure(format!(
                    "authority receipt row {rowid} names a missing {label} history digest {digest}"
                )));
            }
        }
    }
    Ok(())
}

/// Receipt JSON carries the authority digests that the v1 relational row did
/// not repeat. Resolve the referenced binding/definition revisions and bind
/// those digests before the receipt is rewritten into the slim v2 encoding.
async fn require_receipt_authority(
    conn: &impl QueryExecutor,
    binding_id: &str,
    definition_revision: i64,
    binding_revision: i64,
    context: &str,
) -> Result<(String, String)> {
    let mut rows = conn
        .query(
            "SELECT definitions.definition_digest, bindings.binding_digest
             FROM external_source_states_v1 AS states
             JOIN external_source_binding_revisions_v1 AS bindings
               ON bindings.binding_id = states.binding_id
              AND bindings.binding_revision = ?3
              AND bindings.definition_revision = ?2
             JOIN external_source_definition_revisions_v1 AS definitions
               ON definitions.source_id = states.source_id
              AND definitions.definition_revision = bindings.definition_revision
             WHERE states.binding_id = ?1",
            params![binding_id, definition_revision, binding_revision],
        )
        .await
        .map_err(|error| {
            failure(format!(
                "failed to resolve {context} authority {binding_id}/{definition_revision}/{binding_revision}: {error}"
            ))
        })?;
    let Some(row) = rows.next().await.map_err(|error| {
        failure(format!(
            "failed to read {context} authority {binding_id}/{definition_revision}/{binding_revision}: {error}"
        ))
    })? else {
        return Err(failure(format!(
            "{context} {binding_id}/{definition_revision}/{binding_revision} names missing authority history"
        )));
    };
    let definition_digest = row.get::<String>(0).map_err(|error| {
        failure(format!(
            "failed to decode {context} definition authority {binding_id}: {error}"
        ))
    })?;
    let binding_digest = row.get::<String>(1).map_err(|error| {
        failure(format!(
            "failed to decode {context} binding authority {binding_id}: {error}"
        ))
    })?;
    if definition_digest.is_empty() || binding_digest.is_empty() {
        return Err(failure(format!(
            "{context} {binding_id}/{definition_revision}/{binding_revision} has empty authority digests"
        )));
    }
    Ok((definition_digest, binding_digest))
}

async fn load_mutation_history(
    conn: &impl QueryExecutor,
    binding_id: &str,
    mutation_digest: &str,
) -> Result<(String, String)> {
    let mut rows = conn
        .query(
            "SELECT native_object_digest, mutation_json
             FROM external_source_mutations_v1
             WHERE binding_id = ?1 AND mutation_digest = ?2",
            params![binding_id, mutation_digest],
        )
        .await
        .map_err(|error| {
            failure(format!(
                "failed to read mutation history {binding_id}/{mutation_digest}: {error}"
            ))
        })?;
    let Some(row) = rows.next().await.map_err(|error| {
        failure(format!(
            "failed to read mutation history {binding_id}/{mutation_digest}: {error}"
        ))
    })?
    else {
        return Err(failure(format!(
            "mutation {mutation_digest} for binding {binding_id} is absent from history"
        )));
    };
    let native_object_digest = row.get::<String>(0).map_err(|error| {
        failure(format!(
            "failed to decode mutation history {binding_id}/{mutation_digest}: {error}"
        ))
    })?;
    let mutation_json = row.get::<String>(1).map_err(|error| {
        failure(format!(
            "failed to decode mutation history {binding_id}/{mutation_digest}: {error}"
        ))
    })?;
    if !valid_mutation_json(&mutation_json, Some(mutation_digest)) {
        return Err(failure(format!(
            "mutation history {binding_id}/{mutation_digest} has an invalid encoding"
        )));
    }
    Ok((native_object_digest, mutation_json))
}

async fn require_commit_receipt_history(
    conn: &impl QueryExecutor,
    binding_id: &str,
    receipt_digest: &str,
    context: &str,
) -> Result<(String, String)> {
    let mut rows = conn
        .query(
            "SELECT predecessor_frontier_digest, successor_frontier_digest
             FROM external_source_commit_receipts_v1
             WHERE binding_id = ?1 AND receipt_digest = ?2",
            params![binding_id, receipt_digest],
        )
        .await
        .map_err(|error| {
            failure(format!(
                "failed to verify {context} receipt history {binding_id}/{receipt_digest}: {error}"
            ))
        })?;
    let Some(row) = rows.next().await.map_err(|error| {
        failure(format!(
            "failed to read {context} receipt history {binding_id}/{receipt_digest}: {error}"
        ))
    })?
    else {
        return Err(failure(format!(
            "{context} {binding_id}/{receipt_digest} names a receipt absent from history"
        )));
    };
    let predecessor = row.get::<String>(0).map_err(|error| {
        failure(format!(
            "failed to decode {context} receipt history {binding_id}/{receipt_digest}: {error}"
        ))
    })?;
    let successor = row.get::<String>(1).map_err(|error| {
        failure(format!(
            "failed to decode {context} receipt history {binding_id}/{receipt_digest}: {error}"
        ))
    })?;
    Ok((predecessor, successor))
}

async fn require_projection_history(
    conn: &impl QueryExecutor,
    binding_id: &str,
    projection_digest: &str,
) -> Result<(String, String)> {
    let mut rows = conn
        .query(
            "SELECT predecessor_frontier_digest, successor_frontier_digest
             FROM external_source_projection_publications_v1
             WHERE binding_id = ?1 AND projection_digest = ?2",
            params![binding_id, projection_digest],
        )
        .await
        .map_err(|error| {
            failure(format!(
                "failed to verify projection history {binding_id}/{projection_digest}: {error}"
            ))
        })?;
    let Some(row) = rows.next().await.map_err(|error| {
        failure(format!(
            "failed to read projection history {binding_id}/{projection_digest}: {error}"
        ))
    })?
    else {
        return Err(failure(format!(
            "projection {binding_id}/{projection_digest} names a publication absent from history"
        )));
    };
    let predecessor = row.get::<String>(0).map_err(|error| {
        failure(format!(
            "failed to decode projection history {binding_id}/{projection_digest}: {error}"
        ))
    })?;
    let successor = row.get::<String>(1).map_err(|error| {
        failure(format!(
            "failed to decode projection history {binding_id}/{projection_digest}: {error}"
        ))
    })?;
    Ok((predecessor, successor))
}

async fn require_frontier_history(
    conn: &impl QueryExecutor,
    binding_id: &str,
    frontier_digest: &str,
    context: &str,
) -> Result<()> {
    if frontier_digest == "root" {
        return Ok(());
    }
    let mut rows = conn
        .query(
            "SELECT 1
             FROM external_source_commit_receipts_v1
             WHERE binding_id = ?1
               AND (json_extract(receipt_json, '$.source_frontier.digest') = ?2
                    OR json_extract(receipt_json, '$.prior_source_frontier.digest') = ?2)
             UNION ALL
             SELECT 1
             FROM external_source_projection_publications_v1
             WHERE binding_id = ?1
               AND (json_extract(receipt_json, '$.source_frontier.digest') = ?2
                    OR json_extract(receipt_json, '$.expected_projection_frontier.digest') = ?2)
             LIMIT 1",
            params![binding_id, frontier_digest],
        )
        .await
        .map_err(|error| {
            failure(format!(
                "failed to verify {context} frontier history {binding_id}/{frontier_digest}: {error}"
            ))
        })?;
    if rows
        .next()
        .await
        .map_err(|error| {
            failure(format!(
                "failed to read {context} frontier history {binding_id}/{frontier_digest}: {error}"
            ))
        })?
        .is_none()
    {
        return Err(failure(format!(
            "{context} {binding_id}/{frontier_digest} names a frontier absent from history"
        )));
    }
    Ok(())
}

/// The source-state row retains a full copy of its current source frontier,
/// while the receipt history carries the same frontier under its digest. A
/// digest match alone is insufficient: retaining two different payloads for
/// one digest would make the successor store hydrate different states from
/// the same history key. Compare the JSON values semantically so harmless
/// whitespace or object-key ordering differences do not become false drift.
async fn require_frontier_payload_history(
    conn: &impl QueryExecutor,
    binding_id: &str,
    frontier_digest: &str,
    expected_json: &str,
    context: &str,
) -> Result<()> {
    let expected = serde_json::from_str::<serde_json::Value>(expected_json).map_err(|error| {
        failure(format!(
            "{context} {binding_id}/{frontier_digest} has invalid frontier JSON: {error}"
        ))
    })?;
    let mut rows = conn
        .query(
            "SELECT json_extract(receipt_json, '$.source_frontier')
             FROM external_source_commit_receipts_v1
             WHERE binding_id = ?1
               AND json_extract(receipt_json, '$.source_frontier.digest') = ?2
             UNION ALL
             SELECT json_extract(receipt_json, '$.prior_source_frontier')
             FROM external_source_commit_receipts_v1
             WHERE binding_id = ?1
               AND json_extract(receipt_json, '$.prior_source_frontier.digest') = ?2
             UNION ALL
             SELECT json_extract(receipt_json, '$.source_frontier')
             FROM external_source_projection_publications_v1
             WHERE binding_id = ?1
               AND json_extract(receipt_json, '$.source_frontier.digest') = ?2
             UNION ALL
             SELECT json_extract(receipt_json, '$.expected_projection_frontier')
             FROM external_source_projection_publications_v1
             WHERE binding_id = ?1
               AND json_extract(receipt_json, '$.expected_projection_frontier.digest') = ?2
             LIMIT 1",
            params![binding_id, frontier_digest],
        )
        .await
        .map_err(|error| {
            failure(format!(
                "failed to read {context} frontier payload {binding_id}/{frontier_digest}: {error}"
            ))
        })?;
    let Some(row) = rows.next().await.map_err(|error| {
        failure(format!(
            "failed to read {context} frontier payload {binding_id}/{frontier_digest}: {error}"
        ))
    })?
    else {
        return Err(failure(format!(
            "{context} {binding_id}/{frontier_digest} names a frontier absent from history"
        )));
    };
    let history_json = row.get::<String>(0).map_err(|error| {
        failure(format!(
            "failed to decode {context} frontier payload {binding_id}/{frontier_digest}: {error}"
        ))
    })?;
    let history = serde_json::from_str::<serde_json::Value>(&history_json).map_err(|error| {
        failure(format!(
            "{context} frontier {binding_id}/{frontier_digest} has invalid history JSON: {error}"
        ))
    })?;
    if history != expected {
        return Err(failure(format!(
            "{context} frontier {binding_id}/{frontier_digest} has a conflicting payload"
        )));
    }
    Ok(())
}

async fn validate_mutation_history_rows(conn: &impl QueryExecutor) -> Result<()> {
    let mut rows = conn
        .query(
            "SELECT rowid, binding_id, mutation_digest, native_object_digest,
                    source_receipt_digest, mutation_json
             FROM external_source_mutations_v1 ORDER BY rowid",
            (),
        )
        .await
        .map_err(|error| failure(format!("failed to read mutation history: {error}")))?;
    while let Some(row) = rows
        .next()
        .await
        .map_err(|error| failure(format!("failed to read mutation history row: {error}")))?
    {
        let rowid = row
            .get::<i64>(0)
            .map_err(|error| failure(format!("failed to decode mutation history row: {error}")))?;
        let binding_id = row.get::<String>(1).map_err(|error| {
            failure(format!(
                "failed to decode mutation history row {rowid}: {error}"
            ))
        })?;
        let mutation_digest = row.get::<String>(2).map_err(|error| {
            failure(format!(
                "failed to decode mutation history row {rowid}: {error}"
            ))
        })?;
        let native_object_digest = row.get::<String>(3).map_err(|error| {
            failure(format!(
                "failed to decode mutation history row {rowid}: {error}"
            ))
        })?;
        let source_receipt_digest = row.get::<String>(4).map_err(|error| {
            failure(format!(
                "failed to decode mutation history row {rowid}: {error}"
            ))
        })?;
        let mutation_json = row.get::<String>(5).map_err(|error| {
            failure(format!(
                "failed to decode mutation history row {rowid}: {error}"
            ))
        })?;
        if native_object_digest.is_empty()
            || !valid_mutation_json(&mutation_json, Some(&mutation_digest))
        {
            return Err(failure(format!(
                "mutation history row {rowid} has an invalid immutable encoding"
            )));
        }
        let mutation_value =
            serde_json::from_str::<serde_json::Value>(&mutation_json).map_err(|error| {
                failure(format!(
                    "mutation history row {rowid} has invalid JSON: {error}"
                ))
            })?;
        let mut receipt_rows = conn
            .query(
                "SELECT receipt_json
                 FROM external_source_commit_receipts_v1
                 WHERE binding_id = ?1 AND receipt_digest = ?2",
                params![&binding_id, &source_receipt_digest],
            )
            .await
            .map_err(|error| {
                failure(format!(
                    "failed to read mutation history receipt {binding_id}/{source_receipt_digest}: {error}"
                ))
            })?;
        let Some(receipt_row) = receipt_rows.next().await.map_err(|error| {
            failure(format!(
                "failed to read mutation history receipt {binding_id}/{source_receipt_digest}: {error}"
            ))
        })?
        else {
            // Keep the history reference error separate from the later
            // receipt-content check so a missing receipt remains actionable.
            return Err(failure(format!(
                "mutation history row {rowid} names a missing source receipt {binding_id}/{source_receipt_digest}"
            )));
        };
        let receipt_json = receipt_row.get::<String>(0).map_err(|error| {
            failure(format!(
                "failed to decode mutation history receipt {binding_id}/{source_receipt_digest}: {error}"
            ))
        })?;
        let receipt_value =
            serde_json::from_str::<serde_json::Value>(&receipt_json).map_err(|error| {
                failure(format!(
                    "mutation history row {rowid} names a receipt with invalid JSON: {error}"
                ))
            })?;
        let receipt_contains_mutation = receipt_value
            .get("mutations")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|mutations| mutations.iter().any(|mutation| mutation == &mutation_value));
        if !receipt_contains_mutation {
            return Err(failure(format!(
                "mutation history row {rowid} is not retained by source receipt {binding_id}/{source_receipt_digest}"
            )));
        }
        require_commit_receipt_history(
            conn,
            &binding_id,
            &source_receipt_digest,
            "mutation history",
        )
        .await?;
    }
    Ok(())
}

async fn validate_mutation_copy_rows(conn: &impl QueryExecutor, table: &str) -> Result<()> {
    let sql = match table {
        "external_source_objects_v1" => {
            "SELECT rowid, binding_id, native_object_digest, mutation_digest, mutation_json
             FROM external_source_objects_v1 ORDER BY rowid"
        }
        "external_source_projected_objects_v1" => {
            "SELECT rowid, binding_id, native_object_digest, NULL, mutation_json
             FROM external_source_projected_objects_v1 ORDER BY rowid"
        }
        "external_source_projection_effects_v1" => {
            "SELECT rowid, binding_id, native_object_digest, NULL, mutation_json
             FROM external_source_projection_effects_v1 ORDER BY rowid"
        }
        _ => return Err(failure(format!("unknown mutation-copy table '{table}'"))),
    };
    let mut rows = conn
        .query(sql, ())
        .await
        .map_err(|error| failure(format!("failed to read {table} rows: {error}")))?;
    while let Some(row) = rows
        .next()
        .await
        .map_err(|error| failure(format!("failed to read {table} row: {error}")))?
    {
        let rowid = row
            .get::<i64>(0)
            .map_err(|error| failure(format!("failed to decode {table} row id: {error}")))?;
        let binding_id = row
            .get::<String>(1)
            .map_err(|error| failure(format!("failed to decode {table} row {rowid}: {error}")))?;
        let native_object_digest = row
            .get::<String>(2)
            .map_err(|error| failure(format!("failed to decode {table} row {rowid}: {error}")))?;
        let relational_digest = row.get::<Option<String>>(3).map_err(|error| {
            failure(format!(
                "failed to decode {table} row {rowid} mutation digest: {error}"
            ))
        })?;
        let mutation_json = row
            .get::<String>(4)
            .map_err(|error| failure(format!("failed to decode {table} row {rowid}: {error}")))?;
        let encoded_digest = serde_json::from_str::<serde_json::Value>(&mutation_json)
            .ok()
            .and_then(|value| {
                value
                    .get("mutation_digest")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            });
        let Some(encoded_digest) = encoded_digest else {
            return Err(failure(format!(
                "{table} row {rowid} has no mutation digest in its encoding"
            )));
        };
        if relational_digest
            .as_deref()
            .is_some_and(|relational_digest| relational_digest != encoded_digest)
        {
            return Err(failure(format!(
                "{table} row {rowid} mutation digest disagrees with its encoding"
            )));
        }
        let (history_native_object_digest, history_json) =
            load_mutation_history(conn, &binding_id, &encoded_digest).await?;
        if history_native_object_digest != native_object_digest {
            return Err(failure(format!(
                "{table} row {rowid} names mutation {encoded_digest} for a different native object"
            )));
        }
        let encoded_value =
            serde_json::from_str::<serde_json::Value>(&mutation_json).map_err(|error| {
                failure(format!(
                    "{table} row {rowid} has invalid mutation JSON: {error}"
                ))
            })?;
        let history_value =
            serde_json::from_str::<serde_json::Value>(&history_json).map_err(|error| {
                failure(format!(
                    "mutation history {binding_id}/{encoded_digest} has invalid JSON: {error}"
                ))
            })?;
        if encoded_value != history_value {
            return Err(failure(format!(
                "{table} row {rowid} disagrees with mutation {encoded_digest} in history"
            )));
        }
    }
    Ok(())
}

async fn validate_projection_effect_rows(
    conn: &impl QueryExecutor,
    binding_id: &str,
    projection_digest: &str,
    expected_mutation_digests: &[String],
    expected_effects: &[serde_json::Value],
    rowid: i64,
) -> Result<()> {
    let mut rows = conn
        .query(
            "SELECT effect_json, mutation_json
             FROM external_source_projection_effects_v1
             WHERE binding_id = ?1 AND projection_digest = ?2
             ORDER BY effect_index",
            params![binding_id, projection_digest],
        )
        .await
        .map_err(|error| {
            failure(format!(
                "failed to read projection effects for receipt row {rowid}: {error}"
            ))
        })?;
    let mut actual_effects = Vec::new();
    let mut actual_mutation_digests = Vec::new();
    while let Some(row) = rows.next().await.map_err(|error| {
        failure(format!(
            "failed to read projection effect for receipt row {rowid}: {error}"
        ))
    })? {
        let effect_json = row.get::<String>(0).map_err(|error| {
            failure(format!(
                "failed to decode projection effect for receipt row {rowid}: {error}"
            ))
        })?;
        actual_effects.push(
            serde_json::from_str::<serde_json::Value>(&effect_json).map_err(|error| {
                failure(format!(
                    "projection effect for receipt row {rowid} has invalid JSON: {error}"
                ))
            })?,
        );
        let mutation_json = row.get::<String>(1).map_err(|error| {
            failure(format!(
                "failed to decode projection effect mutation for receipt row {rowid}: {error}"
            ))
        })?;
        let mutation_digest = serde_json::from_str::<serde_json::Value>(&mutation_json)
            .ok()
            .and_then(|value| {
                value
                    .get("mutation_digest")
                    .and_then(serde_json::Value::as_str)
                    .filter(|digest| !digest.is_empty())
                    .map(str::to_owned)
            })
            .ok_or_else(|| {
                failure(format!(
                    "projection effect for receipt row {rowid} has no mutation digest"
                ))
            })?;
        actual_mutation_digests.push(mutation_digest);
    }
    if actual_effects.as_slice() != expected_effects
        || actual_mutation_digests.as_slice() != expected_mutation_digests
    {
        return Err(failure(format!(
            "projection receipt row {rowid} effects or mutations disagree with their retained history"
        )));
    }
    Ok(())
}

async fn validate_json_column(
    conn: &impl QueryExecutor,
    table: &str,
    projection: bool,
) -> Result<()> {
    let sql = if projection {
        "SELECT rowid, binding_id, projection_digest, source_receipt_digest,
                predecessor_frontier_digest, successor_frontier_digest, receipt_json
         FROM external_source_projection_publications_v1 ORDER BY rowid"
    } else {
        "SELECT rowid, binding_id, idempotency_key, request_digest,
                definition_revision, binding_revision,
                predecessor_frontier_digest, successor_frontier_digest,
                receipt_digest, receipt_json
         FROM external_source_commit_receipts_v1 ORDER BY rowid"
    };
    let mut rows = conn
        .query(sql, ())
        .await
        .map_err(|error| failure(format!("failed to read {table} rows: {error}")))?;
    while let Some(row) = rows
        .next()
        .await
        .map_err(|error| failure(format!("failed to read {table} row: {error}")))?
    {
        let rowid = row
            .get::<i64>(0)
            .map_err(|error| failure(format!("failed to decode {table} row id: {error}")))?;
        let binding_id = row
            .get::<String>(1)
            .map_err(|error| failure(format!("failed to decode {table} row {rowid}: {error}")))?;
        let (columns, raw) = if projection {
            let projection_digest = row.get::<String>(2).map_err(|error| {
                failure(format!("failed to decode {table} row {rowid}: {error}"))
            })?;
            let source_receipt_digest = row.get::<String>(3).map_err(|error| {
                failure(format!("failed to decode {table} row {rowid}: {error}"))
            })?;
            let predecessor = row.get::<String>(4).map_err(|error| {
                failure(format!("failed to decode {table} row {rowid}: {error}"))
            })?;
            let successor = row.get::<String>(5).map_err(|error| {
                failure(format!("failed to decode {table} row {rowid}: {error}"))
            })?;
            let raw = row.get::<String>(6).map_err(|error| {
                failure(format!("failed to decode {table} row {rowid}: {error}"))
            })?;
            (
                ReceiptColumns {
                    idempotency_key: None,
                    request_digest: None,
                    definition_revision: None,
                    definition_digest: None,
                    binding_revision: None,
                    binding_digest: None,
                    projection_digest: Some(projection_digest.clone()),
                    source_receipt_digest: Some(source_receipt_digest),
                    predecessor_frontier_digest: predecessor,
                    successor_frontier_digest: successor,
                    receipt_digest: projection_digest,
                },
                raw,
            )
        } else {
            let idempotency_key = row.get::<String>(2).map_err(|error| {
                failure(format!("failed to decode {table} row {rowid}: {error}"))
            })?;
            let request_digest = row.get::<String>(3).map_err(|error| {
                failure(format!("failed to decode {table} row {rowid}: {error}"))
            })?;
            let definition_revision = row.get::<i64>(4).map_err(|error| {
                failure(format!("failed to decode {table} row {rowid}: {error}"))
            })?;
            let binding_revision = row.get::<i64>(5).map_err(|error| {
                failure(format!("failed to decode {table} row {rowid}: {error}"))
            })?;
            let predecessor = row.get::<String>(6).map_err(|error| {
                failure(format!("failed to decode {table} row {rowid}: {error}"))
            })?;
            let successor = row.get::<String>(7).map_err(|error| {
                failure(format!("failed to decode {table} row {rowid}: {error}"))
            })?;
            let receipt_digest = row.get::<String>(8).map_err(|error| {
                failure(format!("failed to decode {table} row {rowid}: {error}"))
            })?;
            let raw = row.get::<String>(9).map_err(|error| {
                failure(format!("failed to decode {table} row {rowid}: {error}"))
            })?;
            let (definition_digest, binding_digest) = require_receipt_authority(
                conn,
                &binding_id,
                definition_revision,
                binding_revision,
                "commit receipt",
            )
            .await?;
            (
                ReceiptColumns {
                    idempotency_key: Some(idempotency_key),
                    request_digest: Some(request_digest),
                    definition_revision: Some(definition_revision),
                    definition_digest: Some(definition_digest),
                    binding_revision: Some(binding_revision),
                    binding_digest: Some(binding_digest),
                    projection_digest: None,
                    source_receipt_digest: None,
                    predecessor_frontier_digest: predecessor,
                    successor_frontier_digest: successor,
                    receipt_digest,
                },
                raw,
            )
        };
        if !valid_receipt_json(&raw, projection, &columns) {
            return Err(failure(format!(
                "{table} row {rowid} has no losslessly convertible receipt JSON"
            )));
        }
        let value = serde_json::from_str::<serde_json::Value>(&raw).map_err(|error| {
            failure(format!(
                "failed to parse {table} row {rowid} receipt JSON: {error}"
            ))
        })?;
        let mutations = value
            .get("mutations")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| failure(format!("{table} row {rowid} has no mutation list")))?;
        if projection {
            let object = value.as_object().ok_or_else(|| {
                failure(format!("{table} row {rowid} receipt JSON is not an object"))
            })?;
            let projection_digest = columns
                .projection_digest
                .as_deref()
                .ok_or_else(|| failure(format!("{table} row {rowid} has no projection digest")))?;
            let expected_effects = object
                .get("effects")
                .and_then(serde_json::Value::as_array)
                .cloned()
                .ok_or_else(|| {
                    failure(format!(
                        "{table} row {rowid} has no effect list for lossless conversion"
                    ))
                })?;
            let expected_mutation_digests = mutations
                .iter()
                .map(|mutation| {
                    mutation
                        .get("mutation_digest")
                        .and_then(serde_json::Value::as_str)
                        .filter(|digest| !digest.is_empty())
                        .map(str::to_owned)
                        .ok_or_else(|| {
                            failure(format!(
                                "{table} row {rowid} mutation has no digest for effect history"
                            ))
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            validate_projection_effect_rows(
                conn,
                &binding_id,
                projection_digest,
                &expected_mutation_digests,
                &expected_effects,
                rowid,
            )
            .await?;
            let definition_revision = object
                .get("definition_revision")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| {
                    failure(format!("{table} row {rowid} has no definition revision"))
                })?;
            let binding_revision = object
                .get("binding_revision")
                .and_then(serde_json::Value::as_i64)
                .ok_or_else(|| failure(format!("{table} row {rowid} has no binding revision")))?;
            let (definition_digest, binding_digest) = require_receipt_authority(
                conn,
                &binding_id,
                definition_revision,
                binding_revision,
                "projection receipt",
            )
            .await?;
            if !object_string_matches(object, "definition_digest", Some(&definition_digest))
                || !object_string_matches(object, "binding_digest", Some(&binding_digest))
            {
                return Err(failure(format!(
                    "{table} row {rowid} disagrees with its authority history"
                )));
            }
        }
        for (index, mutation) in mutations.iter().enumerate() {
            let mutation_digest = mutation
                .get("mutation_digest")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| {
                    failure(format!(
                        "{table} row {rowid} mutation {index} has no digest"
                    ))
                })?;
            let (_, history_json) = load_mutation_history(conn, &binding_id, mutation_digest)
                .await
                .map_err(|error| {
                    failure(format!(
                        "{table} row {rowid} mutation {index} is not losslessly referenced: {error}"
                    ))
                })?;
            let history_value =
                serde_json::from_str::<serde_json::Value>(&history_json).map_err(|error| {
                    failure(format!(
                        "{table} row {rowid} mutation {index} has invalid history JSON: {error}"
                    ))
                })?;
            if mutation != &history_value {
                return Err(failure(format!(
                    "{table} row {rowid} mutation {index} disagrees with its history"
                )));
            }
        }
        if projection {
            let source_receipt_digest =
                columns.source_receipt_digest.as_deref().ok_or_else(|| {
                    failure(format!("{table} row {rowid} has no source receipt digest"))
                })?;
            let (_, commit_successor) = require_commit_receipt_history(
                conn,
                &binding_id,
                source_receipt_digest,
                "projection publication",
            )
            .await?;
            if commit_successor != columns.successor_frontier_digest {
                return Err(failure(format!(
                    "{table} row {rowid} successor frontier disagrees with its source receipt"
                )));
            }
        }
    }
    Ok(())
}

async fn validate_source_rows(conn: &impl QueryExecutor) -> Result<()> {
    validate_authority_rows(conn).await?;
    validate_mutation_history_rows(conn).await?;
    validate_mutation_copy_rows(conn, "external_source_objects_v1").await?;
    validate_mutation_copy_rows(conn, "external_source_projected_objects_v1").await?;
    validate_mutation_copy_rows(conn, "external_source_projection_effects_v1").await?;
    validate_json_column(conn, "external_source_commit_receipts_v1", false).await?;
    validate_json_column(conn, "external_source_projection_publications_v1", true).await?;
    validate_external_source_history_refs(conn).await?;
    validate_frontier_rows(conn).await?;
    Ok(())
}

async fn validate_external_source_history_refs(conn: &impl QueryExecutor) -> Result<()> {
    let mut rows = conn
        .query(
            "SELECT effects.rowid, effects.binding_id, effects.projection_digest
             FROM external_source_projection_effects_v1 AS effects
             LEFT JOIN external_source_projection_publications_v1 AS publications
               ON publications.binding_id = effects.binding_id
              AND publications.projection_digest = effects.projection_digest
             WHERE publications.projection_digest IS NULL
             ORDER BY effects.rowid
             LIMIT 1",
            (),
        )
        .await
        .map_err(|error| {
            failure(format!(
                "failed to read external projection effects: {error}"
            ))
        })?;
    if let Some(row) = rows.next().await.map_err(|error| {
        failure(format!(
            "failed to read external projection effect history: {error}"
        ))
    })? {
        let rowid = row.get::<i64>(0).map_err(|error| {
            failure(format!(
                "failed to decode external projection effect history row: {error}"
            ))
        })?;
        let binding_id = row.get::<String>(1).map_err(|error| {
            failure(format!(
                "failed to decode external projection effect history row {rowid}: {error}"
            ))
        })?;
        let projection_digest = row.get::<String>(2).map_err(|error| {
            failure(format!(
                "failed to decode external projection effect history row {rowid}: {error}"
            ))
        })?;
        return Err(failure(format!(
            "external projection effect row {rowid} names missing projection history {binding_id}/{projection_digest}"
        )));
    }

    let mut rows = conn
        .query(
            "SELECT rowid, binding_id, source_receipt_digest
             FROM external_source_lineage_v1 ORDER BY rowid",
            (),
        )
        .await
        .map_err(|error| failure(format!("failed to read external lineage history: {error}")))?;
    while let Some(row) = rows.next().await.map_err(|error| {
        failure(format!(
            "failed to read external lineage history row: {error}"
        ))
    })? {
        let rowid = row.get::<i64>(0).map_err(|error| {
            failure(format!(
                "failed to decode external lineage history row: {error}"
            ))
        })?;
        let binding_id = row.get::<String>(1).map_err(|error| {
            failure(format!(
                "failed to decode external lineage history row {rowid}: {error}"
            ))
        })?;
        let receipt_digest = row.get::<String>(2).map_err(|error| {
            failure(format!(
                "failed to decode external lineage history row {rowid}: {error}"
            ))
        })?;
        require_commit_receipt_history(conn, &binding_id, &receipt_digest, "lineage").await?;
    }
    validate_lineage_membership(conn, false).await?;

    let mut rows = conn
        .query(
            "SELECT rowid, binding_id, projection_digest
             FROM external_source_projection_lineage_v1 ORDER BY rowid",
            (),
        )
        .await
        .map_err(|error| {
            failure(format!(
                "failed to read external projection lineage history: {error}"
            ))
        })?;
    while let Some(row) = rows.next().await.map_err(|error| {
        failure(format!(
            "failed to read external projection lineage history row: {error}"
        ))
    })? {
        let rowid = row.get::<i64>(0).map_err(|error| {
            failure(format!(
                "failed to decode external projection lineage history row: {error}"
            ))
        })?;
        let binding_id = row.get::<String>(1).map_err(|error| {
            failure(format!(
                "failed to decode external projection lineage history row {rowid}: {error}"
            ))
        })?;
        let projection_digest = row.get::<String>(2).map_err(|error| {
            failure(format!(
                "failed to decode external projection lineage history row {rowid}: {error}"
            ))
        })?;
        require_projection_history(conn, &binding_id, &projection_digest).await?;
    }
    validate_lineage_membership(conn, true).await?;

    let mut rows = conn
        .query(
            "SELECT rowid, binding_id, predecessor_frontier_digest,
                    successor_frontier_digest, source_receipt_digest
             FROM external_source_pending_projections_v1 ORDER BY rowid",
            (),
        )
        .await
        .map_err(|error| {
            failure(format!(
                "failed to read external pending projections: {error}"
            ))
        })?;
    while let Some(row) = rows.next().await.map_err(|error| {
        failure(format!(
            "failed to read external pending projection row: {error}"
        ))
    })? {
        let rowid = row.get::<i64>(0).map_err(|error| {
            failure(format!(
                "failed to decode external pending projection row: {error}"
            ))
        })?;
        let binding_id = row.get::<String>(1).map_err(|error| {
            failure(format!(
                "failed to decode external pending projection row {rowid}: {error}"
            ))
        })?;
        let predecessor = row.get::<String>(2).map_err(|error| {
            failure(format!(
                "failed to decode external pending projection row {rowid}: {error}"
            ))
        })?;
        let successor = row.get::<String>(3).map_err(|error| {
            failure(format!(
                "failed to decode external pending projection row {rowid}: {error}"
            ))
        })?;
        let receipt_digest = row.get::<String>(4).map_err(|error| {
            failure(format!(
                "failed to decode external pending projection row {rowid}: {error}"
            ))
        })?;
        let (receipt_predecessor, receipt_successor) = require_commit_receipt_history(
            conn,
            &binding_id,
            &receipt_digest,
            "pending projection",
        )
        .await?;
        if receipt_predecessor != predecessor || receipt_successor != successor {
            return Err(failure(format!(
                "pending projection row {rowid} disagrees with its source receipt"
            )));
        }
        require_frontier_history(conn, &binding_id, &predecessor, "pending projection").await?;
        require_frontier_history(conn, &binding_id, &successor, "pending projection").await?;
    }

    let mut rows = conn
        .query(
            "SELECT rowid, binding_id, source_id,
                    definition_revision, definition_digest,
                    binding_revision, binding_digest,
                    source_frontier_digest, source_frontier_json,
                    projection_frontier_digest, latest_source_receipt_digest,
                    latest_projection_receipt_digest
             FROM external_source_states_v1 ORDER BY rowid",
            (),
        )
        .await
        .map_err(|error| failure(format!("failed to read external source states: {error}")))?;
    while let Some(row) = rows
        .next()
        .await
        .map_err(|error| failure(format!("failed to read external source state row: {error}")))?
    {
        let rowid = row.get::<i64>(0).map_err(|error| {
            failure(format!(
                "failed to decode external source state row: {error}"
            ))
        })?;
        let binding_id = row.get::<String>(1).map_err(|error| {
            failure(format!(
                "failed to decode external source state row {rowid}: {error}"
            ))
        })?;
        let source_id = row.get::<String>(2).map_err(|error| {
            failure(format!(
                "failed to decode external source state row {rowid}: {error}"
            ))
        })?;
        let definition_revision = row.get::<i64>(3).map_err(|error| {
            failure(format!(
                "failed to decode external source state row {rowid}: {error}"
            ))
        })?;
        let definition_digest = row.get::<String>(4).map_err(|error| {
            failure(format!(
                "failed to decode external source state row {rowid}: {error}"
            ))
        })?;
        let binding_revision = row.get::<i64>(5).map_err(|error| {
            failure(format!(
                "failed to decode external source state row {rowid}: {error}"
            ))
        })?;
        let binding_digest = row.get::<String>(6).map_err(|error| {
            failure(format!(
                "failed to decode external source state row {rowid}: {error}"
            ))
        })?;
        let source_frontier = row.get::<String>(7).map_err(|error| {
            failure(format!(
                "failed to decode external source state row {rowid}: {error}"
            ))
        })?;
        let source_frontier_json = row.get::<String>(8).map_err(|error| {
            failure(format!(
                "failed to decode external source state row {rowid}: {error}"
            ))
        })?;
        let projection_frontier = row.get::<Option<String>>(9).map_err(|error| {
            failure(format!(
                "failed to decode external source state row {rowid}: {error}"
            ))
        })?;
        let latest_source_receipt = row.get::<String>(10).map_err(|error| {
            failure(format!(
                "failed to decode external source state row {rowid}: {error}"
            ))
        })?;
        let latest_projection_receipt = row.get::<Option<String>>(11).map_err(|error| {
            failure(format!(
                "failed to decode external source state row {rowid}: {error}"
            ))
        })?;
        let (authoritative_definition_digest, authoritative_binding_digest) =
            require_receipt_authority(
                conn,
                &binding_id,
                definition_revision,
                binding_revision,
                "source state",
            )
            .await?;
        if definition_digest != authoritative_definition_digest
            || binding_digest != authoritative_binding_digest
        {
            return Err(failure(format!(
                "source state row {rowid} disagrees with its authority history"
            )));
        }
        let source_frontier_value =
            serde_json::from_str::<serde_json::Value>(&source_frontier_json).map_err(|error| {
                failure(format!(
                    "external source state row {rowid} has invalid source frontier JSON: {error}"
                ))
            })?;
        if !nonempty_digest(source_frontier_value.get("digest"))
            || source_frontier_value
                .get("digest")
                .and_then(serde_json::Value::as_str)
                != Some(source_frontier.as_str())
        {
            return Err(failure(format!(
                "source state row {rowid} source frontier disagrees with its JSON"
            )));
        }
        if source_id.is_empty()
            || (projection_frontier.is_some() != latest_projection_receipt.is_some())
        {
            return Err(failure(format!(
                "source state row {rowid} has an incomplete source identity or projection history"
            )));
        }
        let (_, source_receipt_successor) = require_commit_receipt_history(
            conn,
            &binding_id,
            &latest_source_receipt,
            "source state",
        )
        .await?;
        if source_receipt_successor != source_frontier {
            return Err(failure(format!(
                "source state row {rowid} source frontier disagrees with its latest receipt"
            )));
        }
        require_frontier_history(conn, &binding_id, &source_frontier, "source state").await?;
        if source_frontier != "root" {
            require_frontier_payload_history(
                conn,
                &binding_id,
                &source_frontier,
                &source_frontier_json,
                "source state",
            )
            .await?;
        }
        if let Some(projection_frontier) = projection_frontier {
            let (_, projection_successor) = require_projection_history(
                conn,
                &binding_id,
                latest_projection_receipt.as_deref().ok_or_else(|| {
                    failure(format!(
                        "source state row {rowid} has a projection frontier without a latest projection receipt"
                    ))
                })?,
            )
            .await?;
            if projection_successor != projection_frontier {
                return Err(failure(format!(
                    "source state row {rowid} projection frontier disagrees with its latest projection receipt"
                )));
            }
            require_frontier_history(conn, &binding_id, &projection_frontier, "source state")
                .await?;
        }
        if let Some(latest_projection_receipt) = latest_projection_receipt {
            require_projection_history(conn, &binding_id, &latest_projection_receipt).await?;
        }
    }
    Ok(())
}

/// Lineage rows are retained as full history while receipts keep the same
/// lineage objects inline. Both copies must agree before the receipt is
/// rewritten or the source lineage row can become an orphaned, contradictory
/// record in the successor store.
async fn validate_lineage_membership(conn: &impl QueryExecutor, projection: bool) -> Result<()> {
    let (table, parent_table, parent_key) = if projection {
        (
            "external_source_projection_lineage_v1",
            "external_source_projection_publications_v1",
            "projection_digest",
        )
    } else {
        (
            "external_source_lineage_v1",
            "external_source_commit_receipts_v1",
            "receipt_digest",
        )
    };
    let sql = if projection {
        "SELECT rowid, binding_id, projection_digest, lineage_digest, lineage_json
         FROM external_source_projection_lineage_v1 ORDER BY rowid"
    } else {
        "SELECT rowid, binding_id, source_receipt_digest, lineage_digest, lineage_json
         FROM external_source_lineage_v1 ORDER BY rowid"
    };
    let mut rows = conn
        .query(sql, ())
        .await
        .map_err(|error| failure(format!("failed to read {table} lineage members: {error}")))?;
    while let Some(row) = rows
        .next()
        .await
        .map_err(|error| failure(format!("failed to read {table} lineage member: {error}")))?
    {
        let rowid = row
            .get::<i64>(0)
            .map_err(|error| failure(format!("failed to decode {table} lineage row: {error}")))?;
        let binding_id = row.get::<String>(1).map_err(|error| {
            failure(format!(
                "failed to decode {table} lineage row {rowid}: {error}"
            ))
        })?;
        let parent_digest = row.get::<String>(2).map_err(|error| {
            failure(format!(
                "failed to decode {table} lineage row {rowid}: {error}"
            ))
        })?;
        let lineage_digest = row.get::<String>(3).map_err(|error| {
            failure(format!(
                "failed to decode {table} lineage row {rowid}: {error}"
            ))
        })?;
        let lineage_json = row.get::<String>(4).map_err(|error| {
            failure(format!(
                "failed to decode {table} lineage row {rowid}: {error}"
            ))
        })?;
        let lineage =
            serde_json::from_str::<serde_json::Value>(&lineage_json).map_err(|error| {
                failure(format!(
                    "{table} lineage row {rowid} has invalid JSON: {error}"
                ))
            })?;
        if lineage
            .get("lineage_digest")
            .and_then(serde_json::Value::as_str)
            != Some(lineage_digest.as_str())
        {
            return Err(failure(format!(
                "{table} lineage row {rowid} disagrees with its lineage digest"
            )));
        }
        let parent_sql = format!(
            "SELECT receipt_json FROM {parent_table}
             WHERE binding_id = ?1 AND {parent_key} = ?2"
        );
        let mut parent_rows = conn
            .query(&parent_sql, params![&binding_id, &parent_digest])
            .await
            .map_err(|error| {
                failure(format!(
                    "failed to read {table} lineage parent {binding_id}/{parent_digest}: {error}"
                ))
            })?;
        let Some(parent_row) = parent_rows.next().await.map_err(|error| {
            failure(format!(
                "failed to read {table} lineage parent {binding_id}/{parent_digest}: {error}"
            ))
        })?
        else {
            return Err(failure(format!(
                "{table} lineage row {rowid} names a missing parent {binding_id}/{parent_digest}"
            )));
        };
        let parent_json = parent_row.get::<String>(0).map_err(|error| {
            failure(format!(
                "failed to decode {table} lineage parent {binding_id}/{parent_digest}: {error}"
            ))
        })?;
        let parent = serde_json::from_str::<serde_json::Value>(&parent_json).map_err(|error| {
            failure(format!(
                "{table} lineage parent {binding_id}/{parent_digest} has invalid JSON: {error}"
            ))
        })?;
        if !parent
            .get("lineage")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|lineages| lineages.iter().any(|candidate| candidate == &lineage))
        {
            return Err(failure(format!(
                "{table} lineage row {rowid} is not retained by parent {binding_id}/{parent_digest}"
            )));
        }
    }
    Ok(())
}

/// The normalized frontier table keys by `(binding_id, frontier_digest)`, so
/// two released receipts may share a frontier only when they carry the exact
/// same frontier payload. Reject conflicting copies before any successor row
/// is written; `INSERT OR IGNORE` below then only coalesces byte-identical
/// repeated frontiers.
async fn validate_frontier_rows(conn: &impl QueryExecutor) -> Result<()> {
    let mut rows = conn
        .query(
            "SELECT binding_id, frontier_digest
             FROM (
                 SELECT binding_id,
                        json_extract(receipt_json, '$.source_frontier.digest') AS frontier_digest,
                        json_extract(receipt_json, '$.source_frontier') AS frontier_json
                 FROM external_source_commit_receipts_v1
                 UNION ALL
                 SELECT binding_id,
                        json_extract(receipt_json, '$.prior_source_frontier.digest'),
                        json_extract(receipt_json, '$.prior_source_frontier')
                 FROM external_source_commit_receipts_v1
                 WHERE json_extract(receipt_json, '$.prior_source_frontier.digest') IS NOT NULL
                 UNION ALL
                 SELECT binding_id,
                        json_extract(receipt_json, '$.source_frontier.digest'),
                        json_extract(receipt_json, '$.source_frontier')
                 FROM external_source_projection_publications_v1
                 UNION ALL
                 SELECT binding_id,
                        json_extract(receipt_json, '$.expected_projection_frontier.digest'),
                        json_extract(receipt_json, '$.expected_projection_frontier')
                 FROM external_source_projection_publications_v1
                 WHERE json_extract(receipt_json, '$.expected_projection_frontier.digest') IS NOT NULL
             )
             WHERE frontier_digest IS NOT NULL
             GROUP BY binding_id, frontier_digest
             HAVING COUNT(DISTINCT frontier_json) > 1
             ORDER BY binding_id, frontier_digest
             LIMIT 1",
            (),
        )
        .await
        .map_err(|error| failure(format!("failed to check released frontier consistency: {error}")))?;
    if let Some(row) = rows.next().await.map_err(|error| {
        failure(format!(
            "failed to read released frontier consistency: {error}"
        ))
    })? {
        let binding_id = row.get::<String>(0).map_err(|error| {
            failure(format!(
                "failed to decode conflicting frontier binding: {error}"
            ))
        })?;
        let frontier_digest = row.get::<String>(1).map_err(|error| {
            failure(format!(
                "failed to decode conflicting frontier digest: {error}"
            ))
        })?;
        return Err(failure(format!(
            "released frontier {frontier_digest} for binding {binding_id} has conflicting payloads"
        )));
    }
    Ok(())
}

async fn migrate_external_source(conn: &(impl Executor + Sync)) -> Result<()> {
    execute_batch(
        conn,
        "INSERT INTO external_source_objects_v2(
             binding_id, native_object_digest, partition_digest, mutation_digest
         )
         SELECT binding_id, native_object_digest, partition_digest, mutation_digest
         FROM external_source_objects_v1;
         INSERT INTO external_source_projected_objects_v2(
             binding_id, native_object_digest, mutation_digest
         )
         SELECT binding_id, native_object_digest,
                json_extract(mutation_json, '$.mutation_digest')
         FROM external_source_projected_objects_v1;
         INSERT INTO external_source_projection_effects_v2(
             binding_id, projection_digest, effect_index,
             native_object_digest, mutation_digest, effect_json
         )
         SELECT binding_id, projection_digest, effect_index, native_object_digest,
                json_extract(mutation_json, '$.mutation_digest'), effect_json
         FROM external_source_projection_effects_v1;
         INSERT INTO external_source_frontiers_v1(
             binding_id, frontier_digest, frontier_json
         )
         SELECT DISTINCT binding_id, frontier_digest, frontier_json
         FROM (
             SELECT binding_id,
                    json_extract(receipt_json, '$.source_frontier.digest') AS frontier_digest,
                    json_extract(receipt_json, '$.source_frontier') AS frontier_json
             FROM external_source_commit_receipts_v1
             UNION ALL
             SELECT binding_id,
                    json_extract(receipt_json, '$.prior_source_frontier.digest'),
                    json_extract(receipt_json, '$.prior_source_frontier')
             FROM external_source_commit_receipts_v1
             WHERE json_extract(receipt_json, '$.prior_source_frontier.digest') IS NOT NULL
             UNION ALL
             SELECT binding_id,
                    json_extract(receipt_json, '$.source_frontier.digest'),
                    json_extract(receipt_json, '$.source_frontier')
             FROM external_source_projection_publications_v1
             UNION ALL
             SELECT binding_id,
                    json_extract(receipt_json, '$.expected_projection_frontier.digest'),
                    json_extract(receipt_json, '$.expected_projection_frontier')
             FROM external_source_projection_publications_v1
             WHERE json_extract(receipt_json, '$.expected_projection_frontier.digest') IS NOT NULL
         )
         WHERE frontier_digest IS NOT NULL;
         INSERT INTO external_source_commit_receipts_v2(
             binding_id, idempotency_key, request_digest, definition_revision,
             binding_revision, predecessor_frontier_digest, successor_frontier_digest,
             receipt_digest, receipt_json
         )
         SELECT binding_id, idempotency_key, request_digest, definition_revision,
                binding_revision, predecessor_frontier_digest, successor_frontier_digest,
                receipt_digest,
                json_set(
                    receipt_json,
                    '$.mutations', json((
                        SELECT json_group_array(json_extract(mutation.value, '$.mutation_digest'))
                        FROM json_each(receipt_json, '$.mutations') AS mutation
                    )),
                    '$.source_frontier',
                    json_extract(receipt_json, '$.source_frontier.digest'),
                    '$.prior_source_frontier',
                    json_extract(receipt_json, '$.prior_source_frontier.digest')
                )
         FROM external_source_commit_receipts_v1;
         INSERT INTO external_source_projection_publications_v2(
             binding_id, projection_digest, source_receipt_digest,
             predecessor_frontier_digest, successor_frontier_digest, receipt_json
         )
         SELECT binding_id, projection_digest, source_receipt_digest,
                predecessor_frontier_digest, successor_frontier_digest,
                json_set(
                    receipt_json,
                    '$.mutations', json((
                        SELECT json_group_array(json_extract(mutation.value, '$.mutation_digest'))
                        FROM json_each(receipt_json, '$.mutations') AS mutation
                    )),
                    '$.effects', json('[]'),
                    '$.source_frontier',
                    json_extract(receipt_json, '$.source_frontier.digest'),
                    '$.expected_projection_frontier',
                    json_extract(receipt_json, '$.expected_projection_frontier.digest')
                )
         FROM external_source_projection_publications_v1;
         DROP TABLE external_source_projection_effects_v1;
         DROP TABLE external_source_projected_objects_v1;
         DROP TABLE external_source_objects_v1;
         DROP TABLE external_source_commit_receipts_v1;
         DROP TABLE external_source_projection_publications_v1;",
    )
    .await
}

async fn migrate_runtime_ledger(conn: &(impl Executor + Sync)) -> Result<()> {
    execute_batch(
        conn,
        "INSERT INTO td_runtime_writer_idempotency_v2(
             shard_json, incarnation, authority_epoch, idempotency_key, request_digest,
             original_receipt_json, transaction_scope_json, operation_id,
             durability_json, committed_at_micros
         )
         SELECT shard_json, incarnation, authority_epoch, idempotency_key, request_digest,
                original_receipt_json, transaction_scope_json, operation_id,
                durability_json, committed_at_micros
         FROM td_runtime_writer_idempotency_v1;
         DROP TABLE td_runtime_writer_idempotency_v1;",
    )
    .await
}

async fn drop_retired_semantic_projection(conn: &(impl Executor + Sync)) -> Result<()> {
    execute_batch(
        conn,
        "DROP TRIGGER IF EXISTS semantic_vector_replay_stage_identity_guard;
         DROP TABLE semantic_vector_stage_chunk_receipts;
         DROP TABLE semantic_vector_stage_graph_effects;
         DROP TABLE semantic_vector_stage_batches;
         DROP TABLE semantic_vector_source_scope_bindings;
         DROP TABLE semantic_vector_stage_adoption_authority;
         DROP TABLE semantic_vector_stage_census_authority;
         DROP TABLE semantic_vector_retirement_cleanup;
         DROP TABLE semantic_vector_stages;",
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::RELEASED_SHAPES;

    #[test]
    fn released_inventory_counts_are_pinned_to_tagged_shapes() {
        let shapes = RELEASED_SHAPES
            .as_ref()
            .expect("released source fixtures must build");
        assert_eq!(shapes.v34.len(), 183);
        assert_eq!(shapes.v35_legacy.len(), 187);
        assert_eq!(shapes.v35.len(), 190);
        assert_eq!(shapes.v35_shipped_alias.len(), 190);
    }
}
