//! Convergence for a v34-stamped project store whose inventory is free of
//! retired projections.
//!
//! Every release from v0.1.0-beta.25 through v0.1.0-beta.37 created one
//! byte-identical `tracedecay.db` stamped `user_version` 34 — the exact SQL
//! lives in `tests/fixtures/project-store-released-v34.sql`, whose header
//! carries the tag-to-inventory table. The current contract differs from it in
//! objects that hold no data of their own (two absent indexes, a renamed
//! external-source mutation family, the runtime-writer ledger) and in two
//! diagnostics tables that gained a `publication_revision` column and a wider
//! primary key. Those differences are convergeable and are converged here.
//!
//! Released dense-staging objects remain untouched under exact inventory
//! admission. Removing dense retrieval must not discard unrelated durable
//! project data or the historical publication receipts sharing its store.

use tracedecay_domain::errors::{Result, TraceDecayError};

use crate::db::engine::{Executor, QueryExecutor, params};

const OPERATION: &str = "converge released project schema";

/// One table the released shape carries in a form `SQLite` cannot alter in
/// place: widening a primary key, adding a `NOT NULL` column with no default,
/// and relaxing a `CHECK` all require a rebuild.
///
/// A table is rebuilt when its stored DDL differs from the one the current
/// contract expects, so this list needs no record of which release changed
/// what — [`super::final_shape`] stays the single authority on the expected
/// shape.
struct ReleasedTableRebuild {
    table: &'static str,
    /// The columns the released table carried, written verbatim into the
    /// canonical table so the rebuild is a copy rather than a re-derivation.
    released_columns: &'static str,
    /// The column the canonical shape added, with the value every released
    /// row takes. `None` when only a constraint changed.
    added_column: Option<(&'static str, &'static str)>,
}

/// A canonical DDL batch and every released table it recreates.
///
/// The batch owns the table's indexes and triggers too, which the rebuild's
/// `DROP TABLE` removes, so each group drops all of its tables before
/// replaying the batch once.
struct ReleasedRebuildGroup {
    canonical: &'static str,
    tables: &'static [ReleasedTableRebuild],
}

/// Diagnostics rows published before revisions existed are the first revision
/// of their generation.
const RELEASED_V34_REBUILDS: &[ReleasedRebuildGroup] = &[ReleasedRebuildGroup {
    canonical: tracedecay_store::GENERATION_DIAGNOSTICS_SCHEMA_DDL,
    tables: &[
        ReleasedTableRebuild {
            table: "diagnostic_generation_publications",
            released_columns: "generation_id, record_state, state_generation, published_at",
            added_column: Some(("publication_revision", "1")),
        },
        ReleasedTableRebuild {
            table: "generation_diagnostics",
            released_columns: "diagnostic_anchor, generation_id, repository, worktree, \
                               reference, source_revision, file_occurrence_id, \
                               content_digest, symbol_occurrence_id, span_start, span_end, \
                               code, severity, message, message_digest, producer_kind, \
                               producer, analyzer_revision, configuration_revision, \
                               sanitization_receipt, evidence_class, collected_at, \
                               record_state, state_generation, persisted_at",
            added_column: Some(("publication_revision", "1")),
        },
    ],
}];

fn failure(message: String) -> TraceDecayError {
    TraceDecayError::Database {
        message,
        operation: OPERATION.to_owned(),
    }
}

/// Converges a store stamped with the released version to the shape this
/// binary creates, carrying every row forward.
///
/// A store still carrying a retired projection is refused first, inside the
/// caller's transaction, so the refusal leaves the store byte-identical.
/// Runs before the payload-digest step, whose own admission check requires the
/// current shape everywhere but the digest objects. Idempotent by
/// construction: the rebuilds are selected by the released column being
/// absent, the schema installs are `CREATE ... IF NOT EXISTS`, and the row
/// moves are the same resumable statements the registered stores converge
/// with.
pub(super) async fn converge_released_project_schema(conn: &(impl Executor + Sync)) -> Result<()> {
    super::require_no_retired_sqlite_projection_object(conn).await?;
    let source_version = super::get_version(conn).await?;
    if stored_object_sql(conn, "memory_v2_assertion_payload_digests")
        .await?
        .is_some()
    {
        validate_payload_digest_rows(conn).await?;
    }
    let has_retired_external_sources = stored_object_sql(conn, "external_source_objects_v1")
        .await?
        .is_some();
    if has_retired_external_sources {
        validate_source_rows(conn).await?;
    }
    if source_version == 35 && !has_retired_external_sources {
        // This live stamp already has the final relational schema; only the
        // known alias guard may differ. Do not repair arbitrary v35 drift.
        super::final_shape::require_exact_final_shape_or_shipped_v35_alias_trigger(conn).await?;
    }
    for group in RELEASED_V34_REBUILDS {
        rebuild_released_group(conn, group).await?;
    }
    // Replaces the released alias-immutability trigger, which guarded every
    // update instead of only the fields that must not change.
    crate::db::retrieval_anchor_schema::install_retrieval_anchor_schema(conn, OPERATION).await?;
    crate::db::memory_v2::create_schema(conn, OPERATION).await?;
    crate::db::external_source::install_external_source_schema(conn, OPERATION).await?;
    if has_retired_external_sources {
        migrate_external_source(conn).await?;
    }
    super::install_runtime_writer_ledger(conn, OPERATION).await?;
    if source_version == super::PAYLOAD_DIGEST_STEP_SOURCE_VERSION {
        super::final_shape::require_final_shape_except_payload_digests(conn).await
    } else {
        super::final_shape::require_exact_final_shape(conn).await?;
        super::set_version(conn, super::SCHEMA_VERSION).await
    }
}

/// Rebuilds every table in one group whose stored DDL is not the one the
/// current contract expects.
///
/// The released rows are copied aside, the tables dropped with their indexes
/// and triggers, the canonical batch replayed, and the rows written back
/// through the canonical column list. A table's own triggers are dropped again
/// before the rows return: a census trigger that fired per copied row would
/// count work its authority already records. Replaying the batch afterwards
/// restores them, which is sound because every statement in these batches
/// creates its object only if it is absent.
///
/// A store this binary created has no pending table and pays one catalog probe
/// per listed table.
async fn rebuild_released_group(
    conn: &(impl Executor + Sync),
    group: &ReleasedRebuildGroup,
) -> Result<()> {
    let mut pending = Vec::new();
    for rebuild in group.tables {
        if released_table_pending(conn, rebuild.table).await? {
            pending.push(rebuild);
        }
    }
    if pending.is_empty() {
        // Released stores installed diagnostics lazily; both tables may be absent.
        return batch(conn, group.canonical).await;
    }
    let mut triggers = Vec::new();
    for rebuild in &pending {
        triggers.extend(table_triggers(conn, rebuild.table).await?);
    }
    // Children of a rebuilt table are valid again before this transaction
    // commits, which is when deferred enforcement checks them.
    batch(conn, "PRAGMA defer_foreign_keys = ON;").await?;
    for rebuild in &pending {
        let scratch = scratch_table(rebuild.table);
        batch(
            conn,
            &format!(
                "CREATE TABLE {scratch} AS SELECT * FROM {table};
                 DROP TABLE {table};",
                table = rebuild.table
            ),
        )
        .await?;
    }
    batch(conn, group.canonical).await?;
    for trigger in &triggers {
        batch(conn, &format!("DROP TRIGGER IF EXISTS {trigger};")).await?;
    }
    for rebuild in &pending {
        let scratch = scratch_table(rebuild.table);
        let columns = rebuild.released_columns;
        let (added, value) = match rebuild.added_column {
            Some((added, value)) => (format!("{added}, "), format!("{value}, ")),
            None => (String::new(), String::new()),
        };
        batch(
            conn,
            &format!(
                "INSERT INTO {table}({added}{columns})
                 SELECT {value}{columns} FROM {scratch};
                 DROP TABLE {scratch};",
                table = rebuild.table
            ),
        )
        .await?;
    }
    batch(conn, group.canonical).await
}

/// Reports whether a table exists carrying DDL other than the one this binary
/// creates.
async fn released_table_pending(conn: &impl QueryExecutor, table: &str) -> Result<bool> {
    let Some(expected) = super::final_shape::expected_object_sql(table)? else {
        return Err(failure(format!(
            "'{table}' is not part of the shape this binary creates"
        )));
    };
    Ok(stored_object_sql(conn, table)
        .await?
        .is_some_and(|stored| stored != expected))
}

/// Names the copy a rebuild reads its released rows out of. The copy lives and
/// dies inside the caller's transaction, so an interrupted convergence leaves
/// neither it nor a half-rebuilt table behind.
fn scratch_table(table: &str) -> String {
    format!("{table}_released_v34")
}

async fn batch(conn: &impl Executor, sql: &str) -> Result<()> {
    conn.execute_batch(sql)
        .await
        .map_err(|error| failure(format!("failed to converge released schema: {error}")))
}

async fn execute_batch(conn: &impl Executor, sql: &str) -> Result<()> {
    batch(conn, sql).await
}

async fn stored_object_sql(conn: &impl QueryExecutor, name: &str) -> Result<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT COALESCE(sql, '') FROM sqlite_master WHERE name = ?1",
            params![name],
        )
        .await
        .map_err(|error| failure(format!("failed to read the stored DDL of {name}: {error}")))?;
    let Some(row) = rows.next().await.map_err(|error| {
        failure(format!(
            "failed to decode the stored DDL of {name}: {error}"
        ))
    })?
    else {
        return Ok(None);
    };
    row.get::<String>(0).map(Some).map_err(|error| {
        failure(format!(
            "failed to decode the stored DDL of {name}: {error}"
        ))
    })
}

/// Every trigger defined on one table, in catalog order.
async fn table_triggers(conn: &impl QueryExecutor, table: &str) -> Result<Vec<String>> {
    let mut rows = conn
        .query(
            "SELECT name FROM sqlite_master
             WHERE type = 'trigger' AND tbl_name = ?1 ORDER BY name",
            params![table],
        )
        .await
        .map_err(|error| failure(format!("failed to list the triggers of {table}: {error}")))?;
    let mut triggers = Vec::new();
    while let Some(row) = rows
        .next()
        .await
        .map_err(|error| failure(format!("failed to read the triggers of {table}: {error}")))?
    {
        triggers
            .push(row.get::<String>(0).map_err(|error| {
                failure(format!("failed to decode a {table} trigger: {error}"))
            })?);
    }
    Ok(triggers)
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
