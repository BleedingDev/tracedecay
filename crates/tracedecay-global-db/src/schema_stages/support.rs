use std::collections::BTreeSet;

use tracedecay_runtime_core::db::engine::QueryExecutor;

use super::{global_db_operation_error, project_registry};

/// The last released registry schema did not yet have the primary-root
/// columns. Admission may append those three nullable columns, but it must
/// never append them to a table that has drifted in another way. Keeping this
/// probe read-only lets the caller reject a bad table before configuration or
/// schema installation gets a write transaction.
const RELEASED_CODE_PROJECT_COLUMNS: &[(&str, &str, i64, i64)] = &[
    ("project_id", "TEXT", 0, 1),
    ("canonical_root", "TEXT", 1, 0),
    ("display_root", "TEXT", 1, 0),
    ("git_common_dir", "TEXT", 0, 0),
    ("git_remote_url", "TEXT", 0, 0),
    ("default_branch", "TEXT", 0, 0),
    ("created_at", "INTEGER", 1, 0),
    ("last_seen_at", "INTEGER", 1, 0),
];

const FINAL_CODE_PROJECT_COLUMNS: &[(&str, &str, i64, i64)] = &[
    ("project_id", "TEXT", 0, 1),
    ("canonical_root", "TEXT", 1, 0),
    ("display_root", "TEXT", 1, 0),
    ("primary_root_platform", "TEXT", 0, 0),
    ("primary_root_bytes", "BLOB", 0, 0),
    ("primary_root_last_seen_at", "INTEGER", 0, 0),
    ("git_common_dir", "TEXT", 0, 0),
    ("git_remote_url", "TEXT", 0, 0),
    ("default_branch", "TEXT", 0, 0),
    ("created_at", "INTEGER", 1, 0),
    ("last_seen_at", "INTEGER", 1, 0),
];

// SQLite's additive ALTER TABLE appends columns. A released store therefore
// has the same final metadata after the migration helper runs, but with the
// three primary-root columns at the end rather than in the fresh-install
// position above. Both layouts are sanctioned so a second open remains
// idempotent without rebuilding a populated table.
const MIGRATED_CODE_PROJECT_COLUMNS: &[(&str, &str, i64, i64)] = &[
    ("project_id", "TEXT", 0, 1),
    ("canonical_root", "TEXT", 1, 0),
    ("display_root", "TEXT", 1, 0),
    ("git_common_dir", "TEXT", 0, 0),
    ("git_remote_url", "TEXT", 0, 0),
    ("default_branch", "TEXT", 0, 0),
    ("created_at", "INTEGER", 1, 0),
    ("last_seen_at", "INTEGER", 1, 0),
    ("primary_root_platform", "TEXT", 0, 0),
    ("primary_root_bytes", "BLOB", 0, 0),
    ("primary_root_last_seen_at", "INTEGER", 0, 0),
];

// Keep the complete table definitions beside the PRAGMA column probes. A
// column-only check cannot see table-level CHECK/FK clauses, STRICT, WITHOUT
// ROWID, or a changed PRIMARY KEY declaration, all of which would make the
// additive path unsafe. These are the SQL definitions recorded by SQLite for
// the released and final installers (whitespace is normalized below).
const RELEASED_CODE_PROJECT_SQL: &str = "CREATE TABLE code_projects (
    project_id TEXT PRIMARY KEY,
    canonical_root TEXT NOT NULL,
    display_root TEXT NOT NULL,
    git_common_dir TEXT,
    git_remote_url TEXT,
    default_branch TEXT,
    created_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL
)";

const FINAL_CODE_PROJECT_SQL: &str = "CREATE TABLE code_projects (
    project_id TEXT PRIMARY KEY,
    canonical_root TEXT NOT NULL,
    display_root TEXT NOT NULL,
    primary_root_platform TEXT,
    primary_root_bytes BLOB,
    primary_root_last_seen_at INTEGER,
    git_common_dir TEXT,
    git_remote_url TEXT,
    default_branch TEXT,
    created_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL
)";

const MIGRATED_CODE_PROJECT_SQL: &str = "CREATE TABLE code_projects (
    project_id TEXT PRIMARY KEY,
    canonical_root TEXT NOT NULL,
    display_root TEXT NOT NULL,
    git_common_dir TEXT,
    git_remote_url TEXT,
    default_branch TEXT,
    created_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,
    primary_root_platform TEXT,
    primary_root_bytes BLOB,
    primary_root_last_seen_at INTEGER
)";

const CODE_PROJECT_INDEX_CONTRACTS: &[(&str, &str)] = &[
    (
        "idx_code_projects_last_seen_project",
        "CREATE INDEX idx_code_projects_last_seen_project
         ON code_projects(last_seen_at DESC, project_id)",
    ),
    (
        "idx_code_projects_git_common_dir",
        "CREATE INDEX idx_code_projects_git_common_dir
         ON code_projects(git_common_dir)",
    ),
    (
        "idx_code_projects_canonical_root_project",
        "CREATE INDEX idx_code_projects_canonical_root_project
         ON code_projects(canonical_root, project_id)",
    ),
];

pub(super) async fn require_admissible_code_projects_shape(
    conn: &impl QueryExecutor,
    fresh_store: bool,
) -> tracedecay_domain::errors::Result<()> {
    let mut objects = conn
        .query(
            "SELECT type, sql FROM sqlite_master
             WHERE name = 'code_projects' COLLATE NOCASE",
            (),
        )
        .await
        .map_err(|error| {
            global_db_operation_error("inspect code_projects registry shape", error)
        })?;
    let Some(object) = objects
        .next()
        .await
        .map_err(|error| global_db_operation_error("read code_projects registry shape", error))?
    else {
        // A truly empty file is the only state allowed to create the registry.
        // An existing file with any schema object but no code-project table is
        // an incomplete registry and must be refused before the writer opens.
        return if fresh_store {
            Ok(())
        } else {
            Err(code_projects_shape_reset(
                "code_projects table is missing from an existing registry",
            ))
        };
    };
    let object_type = object
        .get::<String>(0)
        .map_err(|error| global_db_operation_error("decode code_projects registry shape", error))?;
    if !object_type.eq_ignore_ascii_case("table") {
        return Err(code_projects_shape_reset(
            "code_projects has an incompatible object type",
        ));
    }
    let actual_sql = object
        .get::<Option<String>>(1)
        .map_err(|error| global_db_operation_error("decode code_projects registry SQL", error))?;

    let mut rows = conn
        .query(
            "SELECT name, type, \"notnull\", pk, hidden
             FROM pragma_table_xinfo('code_projects')
             ORDER BY cid",
            (),
        )
        .await
        .map_err(|error| {
            global_db_operation_error("inspect code_projects registry columns", error)
        })?;
    let mut actual = Vec::new();
    while let Some(row) = rows
        .next()
        .await
        .map_err(|error| global_db_operation_error("read code_projects registry columns", error))?
    {
        actual.push((
            row.get::<String>(0).map_err(|error| {
                global_db_operation_error("decode code_projects column name", error)
            })?,
            row.get::<String>(1).map_err(|error| {
                global_db_operation_error("decode code_projects column type", error)
            })?,
            row.get::<i64>(2).map_err(|error| {
                global_db_operation_error("decode code_projects column nullability", error)
            })?,
            row.get::<i64>(3).map_err(|error| {
                global_db_operation_error("decode code_projects primary key", error)
            })?,
            row.get::<i64>(4).map_err(|error| {
                global_db_operation_error("decode code_projects hidden column", error)
            })?,
        ));
    }

    let expected_sql = if shape_matches(&actual, RELEASED_CODE_PROJECT_COLUMNS) {
        RELEASED_CODE_PROJECT_SQL
    } else if shape_matches(&actual, FINAL_CODE_PROJECT_COLUMNS) {
        FINAL_CODE_PROJECT_SQL
    } else if shape_matches(&actual, MIGRATED_CODE_PROJECT_COLUMNS) {
        MIGRATED_CODE_PROJECT_SQL
    } else {
        return Err(code_projects_shape_reset(
            "code_projects has an incompatible number of columns or column metadata",
        ));
    };
    if actual_sql
        .as_deref()
        .is_none_or(|sql| compact_sql(sql) != compact_sql(expected_sql))
    {
        return Err(code_projects_shape_reset(
            "code_projects has an incompatible table SQL contract or constraint",
        ));
    }

    validate_code_projects_objects(conn, shape_matches(&actual, RELEASED_CODE_PROJECT_COLUMNS))
        .await
}

async fn validate_code_projects_objects(
    conn: &impl QueryExecutor,
    released_shape: bool,
) -> tracedecay_domain::errors::Result<()> {
    let mut rows = conn
        .query(
            "SELECT type, name, tbl_name, sql
             FROM sqlite_master
             WHERE name NOT LIKE 'sqlite_%'
             ORDER BY type, name, tbl_name",
            (),
        )
        .await
        .map_err(|error| {
            global_db_operation_error("inspect code_projects registry objects", error)
        })?;
    let mut actual = BTreeSet::new();
    let mut actual_sql = Vec::new();
    while let Some(row) = rows
        .next()
        .await
        .map_err(|error| global_db_operation_error("read code_projects registry objects", error))?
    {
        let object_type = row.get::<String>(0).map_err(|error| {
            global_db_operation_error("decode code_projects registry object type", error)
        })?;
        let name = row.get::<String>(1).map_err(|error| {
            global_db_operation_error("decode code_projects registry object name", error)
        })?;
        let table = row.get::<String>(2).map_err(|error| {
            global_db_operation_error("decode code_projects registry object table", error)
        })?;
        let sql = row.get::<Option<String>>(3).map_err(|error| {
            global_db_operation_error("decode code_projects registry object SQL", error)
        })?;
        let is_expected_index_name = CODE_PROJECT_INDEX_CONTRACTS
            .iter()
            .any(|(expected_name, _)| name.eq_ignore_ascii_case(expected_name));
        let belongs_to_code_projects = table.eq_ignore_ascii_case("code_projects")
            || is_expected_index_name
            || (object_type.eq_ignore_ascii_case("view")
                && sql
                    .as_deref()
                    .is_some_and(|sql| sql_mentions_identifier(sql, "code_projects")));
        if belongs_to_code_projects {
            actual.insert((
                object_type.to_ascii_lowercase(),
                name.to_ascii_lowercase(),
                table.to_ascii_lowercase(),
            ));
            actual_sql.push((object_type, name, table, sql));
        }
    }

    let mut released_expected = BTreeSet::new();
    released_expected.insert((
        "table".to_owned(),
        "code_projects".to_owned(),
        "code_projects".to_owned(),
    ));
    let mut final_expected = released_expected.clone();
    for (name, _) in CODE_PROJECT_INDEX_CONTRACTS {
        final_expected.insert((
            "index".to_owned(),
            (*name).to_owned(),
            "code_projects".to_owned(),
        ));
    }
    // v0.0.66 did not create indexes on code_projects. A released table may
    // nevertheless have the final index set when a prior open began the
    // idempotent index stage and was interrupted before adding the columns.
    // Both complete inventories are safe; any partial or extra inventory is
    // refused before that stage can run again.
    let inventory_is_allowed = if released_shape {
        actual == released_expected || actual == final_expected
    } else {
        actual == final_expected
    };
    if !inventory_is_allowed {
        return Err(code_projects_shape_reset(
            "code_projects has an unexpected attached index, trigger, or view",
        ));
    }

    for (object_type, name, _, sql) in actual_sql {
        if !object_type.eq_ignore_ascii_case("index") {
            continue;
        }
        let Some((_, expected_sql)) = CODE_PROJECT_INDEX_CONTRACTS
            .iter()
            .find(|(expected_name, _)| name.eq_ignore_ascii_case(expected_name))
        else {
            continue;
        };
        if sql
            .as_deref()
            .is_none_or(|sql| compact_sql(sql) != compact_sql(expected_sql))
        {
            return Err(code_projects_shape_reset(
                "code_projects has an incompatible attached index SQL contract",
            ));
        }
    }
    Ok(())
}

fn compact_sql(sql: &str) -> String {
    sql.chars()
        .filter(|character| !character.is_ascii_whitespace())
        .flat_map(char::to_lowercase)
        .collect()
}

fn sql_mentions_identifier(sql: &str, identifier: &str) -> bool {
    let identifier = identifier.to_ascii_lowercase();
    sql.to_ascii_lowercase()
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .any(|token| token == identifier)
}

fn shape_matches(
    actual: &[(String, String, i64, i64, i64)],
    expected: &[(&str, &str, i64, i64)],
) -> bool {
    actual.len() == expected.len()
        && actual.iter().all(|(_, _, _, _, hidden)| *hidden == 0)
        && actual.iter().zip(expected).all(
            |((actual_name, actual_type, actual_not_null, actual_primary_key, _), expected)| {
                actual_name.eq_ignore_ascii_case(expected.0)
                    && actual_type.eq_ignore_ascii_case(expected.1)
                    && *actual_not_null == expected.2
                    && *actual_primary_key == expected.3
            },
        )
}

fn code_projects_shape_reset(reason: &str) -> tracedecay_domain::errors::TraceDecayError {
    tracedecay_domain::errors::TraceDecayError::reset_required(
        project_registry::PROJECT_REGISTRY_AUTHORITY,
        reason,
    )
}

#[cfg(test)]
pub(crate) const RELEASED_FIXTURE_PROJECT_ID: &str = "released-profile-project";
#[cfg(test)]
pub(crate) const RELEASED_FIXTURE_SESSION_ID: &str = "released-profile-session";
#[cfg(test)]
pub(crate) const RELEASED_FIXTURE_WORKFLOW_RUN_ID: &str = "released-profile-workflow";
#[cfg(test)]
pub(crate) const RELEASED_FIXTURE_GIT_OID: &str = "released-profile-git-oid";

/// The registry DDL shipped in the tagged v0.0.66 release. Keep this fixture
/// independent from the current installer: the migration test below first
/// installs the unrelated current authorities, then replaces the registry and
/// workflow namespaces with these exact released definitions.
#[cfg(test)]
pub(crate) const RELEASED_V066_REGISTRY_SCHEMA: &str = "
    CREATE TABLE projects (
        path TEXT PRIMARY KEY,
        tokens_saved INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE code_projects (
        project_id TEXT PRIMARY KEY,
        canonical_root TEXT NOT NULL,
        display_root TEXT NOT NULL,
        git_common_dir TEXT,
        git_remote_url TEXT,
        default_branch TEXT,
        created_at INTEGER NOT NULL,
        last_seen_at INTEGER NOT NULL
    );
    CREATE TABLE project_aliases (
        alias_path TEXT PRIMARY KEY,
        project_id TEXT NOT NULL,
        last_seen_at INTEGER NOT NULL,
        FOREIGN KEY(project_id) REFERENCES code_projects(project_id) ON DELETE CASCADE
    );
    CREATE TABLE store_instances (
        store_id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL,
        store_kind TEXT NOT NULL,
        storage_mode TEXT NOT NULL,
        store_relpath TEXT NOT NULL,
        manifest_relpath TEXT,
        created_at INTEGER NOT NULL,
        last_verified_at INTEGER,
        last_write_at INTEGER,
        FOREIGN KEY(project_id) REFERENCES code_projects(project_id) ON DELETE CASCADE
    );
    CREATE TABLE graph_scopes (
        graph_scope_id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL,
        store_id TEXT NOT NULL,
        branch_name TEXT NOT NULL,
        db_relpath TEXT NOT NULL,
        parent_scope_id TEXT,
        last_synced_at INTEGER,
        writable INTEGER NOT NULL DEFAULT 1,
        FOREIGN KEY(project_id) REFERENCES code_projects(project_id) ON DELETE CASCADE,
        FOREIGN KEY(store_id) REFERENCES store_instances(store_id) ON DELETE CASCADE
    );
    CREATE TABLE store_artifacts (
        store_id TEXT NOT NULL,
        artifact_kind TEXT NOT NULL,
        relpath TEXT NOT NULL,
        size_bytes INTEGER,
        schema_version TEXT,
        updated_at INTEGER,
        PRIMARY KEY (store_id, artifact_kind, relpath),
        FOREIGN KEY(store_id) REFERENCES store_instances(store_id) ON DELETE CASCADE
    );
    CREATE INDEX idx_project_aliases_project_id ON project_aliases(project_id);
    CREATE INDEX idx_store_instances_project_id ON store_instances(project_id);
    CREATE INDEX idx_graph_scopes_project_store ON graph_scopes(project_id, store_id);
";

/// The legacy workflow-index DDL shipped with v0.0.66. The newer source
/// journal tables and workflow identity are intentionally absent from this
/// fixture; admission must classify this namespace as `Create` and add those
/// tables only after the released rows have been accepted.
#[cfg(test)]
pub(crate) const RELEASED_V066_WORKFLOW_SCHEMA: &str = "
    CREATE TABLE workflow_runs (
        run_id TEXT PRIMARY KEY,
        parent_session_id TEXT NOT NULL DEFAULT '',
        name TEXT,
        description TEXT,
        phase_json TEXT,
        status TEXT NOT NULL DEFAULT 'unknown'
            CHECK(status IN ('running', 'completed', 'failed', 'unknown')),
        started_ts INTEGER,
        ended_ts INTEGER,
        result_summary TEXT,
        agent_count INTEGER NOT NULL DEFAULT 0,
        created_at INTEGER NOT NULL DEFAULT (unixepoch()),
        updated_at INTEGER NOT NULL DEFAULT (unixepoch())
    );
    CREATE INDEX idx_workflow_runs_parent
        ON workflow_runs(parent_session_id, started_ts);
    CREATE TABLE workflow_agents (
        run_id TEXT NOT NULL,
        agent_label TEXT NOT NULL,
        agent_id TEXT NOT NULL DEFAULT '',
        phase TEXT,
        transcript_path TEXT,
        agent_session_id TEXT,
        status TEXT NOT NULL DEFAULT 'unknown'
            CHECK(status IN ('running', 'completed', 'failed', 'unknown')),
        model TEXT,
        tokens INTEGER NOT NULL DEFAULT 0,
        started_ts INTEGER,
        ended_ts INTEGER,
        created_at INTEGER NOT NULL DEFAULT (unixepoch()),
        updated_at INTEGER NOT NULL DEFAULT (unixepoch()),
        PRIMARY KEY(run_id, agent_label, agent_id)
    );
    CREATE INDEX idx_workflow_agents_run
        ON workflow_agents(run_id, phase);
    CREATE TABLE workflow_index_meta (
        key TEXT PRIMARY KEY,
        value INTEGER NOT NULL,
        updated_at INTEGER NOT NULL DEFAULT (unixepoch())
    );
";

#[cfg(test)]
pub(crate) fn install_released_v066_registry_and_workflow_fixture(
    connection: &rusqlite::Connection,
) {
    // The current test harness supplies the unrelated registered authorities.
    // Remove only the two namespaces whose released definitions differ, then
    // install the exact tag DDL above before inserting any fixture rows.
    let mut drop_sql = String::from("PRAGMA foreign_keys = OFF;\n");
    drop_sql.push_str("DROP TABLE IF EXISTS workflow_schema;\n");
    for table in [
        "workflow_handoffs",
        "workflow_effect_journal",
        "workflow_definition_transition_journal",
        "workflow_definition_disposition",
        "workflow_definition_source_journal",
        "workflow_fan_out_census_journal",
        "workflow_run_journal",
        "workflow_artifact_payloads",
    ] {
        drop_sql.push_str("DROP TABLE IF EXISTS ");
        drop_sql.push_str(table);
        drop_sql.push_str(";\n");
    }
    for index in ["idx_workflow_runs_parent", "idx_workflow_agents_run"] {
        drop_sql.push_str("DROP INDEX IF EXISTS ");
        drop_sql.push_str(index);
        drop_sql.push_str(";\n");
    }
    for table in ["workflow_index_meta", "workflow_agents", "workflow_runs"] {
        drop_sql.push_str("DROP TABLE IF EXISTS ");
        drop_sql.push_str(table);
        drop_sql.push_str(";\n");
    }
    drop_sql.push_str("DROP TABLE IF EXISTS remote_deletion_tombstones;\n");
    for index in [
        "idx_graph_scopes_project_store",
        "idx_store_instances_project_id",
        "idx_project_aliases_project_id",
    ] {
        drop_sql.push_str("DROP INDEX IF EXISTS ");
        drop_sql.push_str(index);
        drop_sql.push_str(";\n");
    }
    for table in [
        "store_artifacts",
        "graph_scopes",
        "store_instances",
        "project_aliases",
        "code_projects",
        "projects",
    ] {
        drop_sql.push_str("DROP TABLE IF EXISTS ");
        drop_sql.push_str(table);
        drop_sql.push_str(";\n");
    }
    drop_sql.push_str("PRAGMA foreign_keys = ON;\n");
    connection.execute_batch(&drop_sql).unwrap();
    connection
        .execute_batch(RELEASED_V066_REGISTRY_SCHEMA)
        .unwrap();
    connection
        .execute_batch(RELEASED_V066_WORKFLOW_SCHEMA)
        .unwrap();
}

#[cfg(test)]
pub(crate) struct ReleasedProfileFixture {
    pub(crate) observation_id: String,
    pub(crate) receipt_id: String,
}

/// Seeds one complete released profile/global row set through the raw SQLite
/// connection used by migration fixtures. The values deliberately cross the
/// registry, transcript, delivery, observation, authorization, workflow, and
/// Git authorities so an additive migration can prove field-level retention
/// instead of only proving that each table still exists.
#[cfg(test)]
pub(crate) fn seed_released_profile_fixture(
    connection: &rusqlite::Connection,
) -> ReleasedProfileFixture {
    seed_released_profile_fixture_with_mode(connection, false)
}

/// Seeds the rowful registry/workflow fixture as it existed in the v0.0.66
/// release: the eight-column `code_projects` table, legacy workflow index, no
/// source-journal tables, and no remote-deletion catalog. The surrounding
/// current authorities are supplied by the test harness so this fixture can
/// exercise the real registered migration boundary without silently replacing
/// the released workflow schema with the current installer.
#[cfg(test)]
pub(crate) fn seed_released_legacy_profile_fixture(
    connection: &rusqlite::Connection,
) -> ReleasedProfileFixture {
    seed_released_profile_fixture_with_mode(connection, true)
}

#[cfg(test)]
fn seed_released_profile_fixture_with_mode(
    connection: &rusqlite::Connection,
    legacy_only: bool,
) -> ReleasedProfileFixture {
    use rusqlite::params;

    connection
        .execute(
            "INSERT INTO projects(path, tokens_saved) VALUES (?1, ?2)",
            params!["/released/profile/root", 77_i64],
        )
        .unwrap();
    if legacy_only {
        connection
            .execute(
                "INSERT INTO code_projects(
                     project_id, canonical_root, display_root,
                     git_common_dir, git_remote_url, default_branch,
                     created_at, last_seen_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    RELEASED_FIXTURE_PROJECT_ID,
                    "/released/profile/root",
                    "/released/profile/display",
                    "/released/profile/.git",
                    "https://example.invalid/released.git",
                    "released-main",
                    1_000_i64,
                    2_000_i64,
                ],
            )
            .unwrap();
    } else {
        connection
            .execute(
                "INSERT INTO code_projects(
                     project_id, canonical_root, display_root,
                     primary_root_platform, primary_root_bytes, primary_root_last_seen_at,
                     git_common_dir, git_remote_url, default_branch, created_at, last_seen_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                params![
                    RELEASED_FIXTURE_PROJECT_ID,
                    "/released/profile/root",
                    "/released/profile/display",
                    "unix",
                    vec![0x72_u8, 0x6f, 0x6f, 0x74],
                    1_701_i64,
                    "/released/profile/.git",
                    "https://example.invalid/released.git",
                    "released-main",
                    1_000_i64,
                    2_000_i64,
                ],
            )
            .unwrap();
    }
    connection
        .execute(
            "INSERT INTO project_aliases(alias_path, project_id, last_seen_at)
             VALUES (?1, ?2, ?3)",
            params![
                "/released/profile/alias",
                RELEASED_FIXTURE_PROJECT_ID,
                2_001_i64
            ],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO store_instances(
                 store_id, project_id, store_kind, storage_mode, store_relpath,
                 manifest_relpath, created_at, last_verified_at, last_write_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                "released-profile-store",
                RELEASED_FIXTURE_PROJECT_ID,
                "graph",
                "private",
                "store/graph",
                "store/graph/manifest.json",
                1_010_i64,
                1_020_i64,
                1_030_i64,
            ],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO graph_scopes(
                 graph_scope_id, project_id, store_id, branch_name, db_relpath,
                 parent_scope_id, last_synced_at, writable
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                "released-profile-scope",
                RELEASED_FIXTURE_PROJECT_ID,
                "released-profile-store",
                "released-main",
                "scopes/released.db",
                Option::<String>::None,
                1_040_i64,
                1_i64,
            ],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO store_artifacts(
                 store_id, artifact_kind, relpath, size_bytes, schema_version, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                "released-profile-store",
                "manifest",
                "manifest.json",
                123_i64,
                "released-v1",
                1_050_i64,
            ],
        )
        .unwrap();
    if !legacy_only {
        connection
            .execute(
                "INSERT INTO remote_deletion_tombstones(
                     profile_id, target_kind, project_id, tombstone_id, recorded_at_micros,
                     cleanup_status, failure_code, failure_phase, retryable
                 ) VALUES (?1, 'project', ?2, ?3, ?4, 'pending', NULL, NULL, NULL)",
                params![
                    "released-profile",
                    RELEASED_FIXTURE_PROJECT_ID,
                    "released-profile-tombstone",
                    1_060_i64,
                ],
            )
            .unwrap();
    }

    connection
        .execute(
            "INSERT INTO sessions(
                 provider, session_id, project_key, project_path, title,
                 started_at, ended_at, transcript_path, metadata_json,
                 parent_session_id, is_subagent, agent_id, parent_tool_use_id
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                "codex",
                RELEASED_FIXTURE_SESSION_ID,
                RELEASED_FIXTURE_PROJECT_ID,
                "/released/profile/display",
                "released transcript",
                1_100_i64,
                1_200_i64,
                "/released/profile/transcript.jsonl",
                "{\"fixture\":true}",
                "released-parent-session",
                0_i64,
                "released-agent",
                "released-tool-use",
            ],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO session_messages(
                 provider, message_id, session_id, role, timestamp, ordinal, text,
                 kind, model, tool_names, source_path, source_offset, metadata_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                "codex",
                "released-profile-message",
                RELEASED_FIXTURE_SESSION_ID,
                "assistant",
                1_150_i64,
                1_i64,
                "released transcript message",
                "assistant",
                "gpt-5.6-codex",
                "terminal",
                "/released/profile/transcript.jsonl",
                42_i64,
                "{\"fixture\":true}",
            ],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO parse_offsets(file_path, byte_offset, mtime, file_id)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                "/released/profile/transcript.jsonl",
                42_i64,
                1_250_i64,
                99_i64
            ],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO savings_ledger(
                 ts, project_path, tool_name, before_tokens, after_tokens
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                1_260_i64,
                "/released/profile/display",
                "released-tool",
                100_i64,
                70_i64,
            ],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO analytics_events(
                 provider, project_id, session_id, timestamp, event_kind,
                 hook_name, tool_name, tool_category, skill_name, hint_category,
                 hint_id, outcome, metadata_json
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                "codex",
                RELEASED_FIXTURE_PROJECT_ID,
                RELEASED_FIXTURE_SESSION_ID,
                1_270_i64,
                "released_event",
                "released_hook",
                "released-tool",
                "terminal",
                "released-skill",
                "released-hint",
                Option::<String>::None,
                "ok",
                "{\"fixture\":true}",
            ],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO observability_emission_outbox(
                 project_id, owner_event_id, owner_fact_json, delivery_envelope_json, state,
                 analytics_event_id
             ) VALUES (?1, ?2, '{}', '{}', 'pending', NULL)",
            params![
                RELEASED_FIXTURE_PROJECT_ID,
                "released-profile-observability"
            ],
        )
        .unwrap();

    connection
        .execute(
            "INSERT INTO delivery_fanout_events(
                 project_id, owner_event_id, surface, event_class, eligible,
                 valid_at_micros, work_attempt_json, work_attempt_digest
             ) VALUES (?1, ?2, 'hook', 'operation_terminal', 1, ?3, '{}', ?4)",
            params![
                RELEASED_FIXTURE_PROJECT_ID,
                "released-profile-delivery",
                1_300_i64,
                "released-work-attempt-digest",
            ],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO delivery_settlements(
                 project_id, owner_event_id, surface, channel_ref, attempted_at_micros,
                 outcome, settled_at_micros, drop_reason, census_json
             ) VALUES (?1, ?2, 'hook', ?3, ?4, 'delivered', ?5, NULL, '{}')",
            params![
                RELEASED_FIXTURE_PROJECT_ID,
                "released-profile-delivery",
                "released-channel",
                1_301_i64,
                1_302_i64,
            ],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO delivery_source_receipts(
                 project_id, receipt_ref, owner_event_id, surface, channel_ref
             ) VALUES (?1, ?2, ?3, 'hook', ?4)",
            params![
                RELEASED_FIXTURE_PROJECT_ID,
                "released-profile-delivery-receipt",
                "released-profile-delivery",
                "released-channel",
            ],
        )
        .unwrap();

    connection
        .execute(
            "INSERT INTO authorized_scope_sets_v1(
                 scope_set_id, revision, digest, canonical_payload
             ) VALUES (?1, ?2, ?3, ?4)",
            params![
                "released-profile-scope-set",
                1_i64,
                "released-scope-digest",
                vec![0x7b_u8, 0x7d_u8],
            ],
        )
        .unwrap();

    if !legacy_only {
        connection
            .execute(
                "INSERT INTO workflow_artifact_payloads(payload_digest, byte_length, payload)
                 VALUES ('released-workflow-payload', 4, X'726f7721')",
                (),
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO workflow_run_journal(
                     run_id, sequence, command_id, event_payload, event_digest
                 ) VALUES ('released-source-workflow', 1, 'released-command', '{}', 'released-event')",
                (),
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO workflow_fan_out_census_journal(
                     run_id, workflow_sequence, observed_at, census_payload,
                     census_digest, observability_settled
                 ) VALUES ('released-source-workflow', 1, 1400, '{}', 'released-census', 1)",
                (),
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO workflow_definition_source_journal(
                     definition_id, definition_version, payload, payload_digest
                 ) VALUES ('released-definition', 1, '{}', 'released-definition-source')",
                (),
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO workflow_definition_disposition(
                     definition_id, definition_version, state, revision, transitioned_at
                 ) VALUES ('released-definition', 1, 'active', 1, 1401)",
                (),
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO workflow_definition_transition_journal(
                     definition_id, definition_version, to_revision, from_revision,
                     operation, from_state, to_state, transitioned_at
                 ) VALUES ('released-definition', 1, 2, 1, 'activate', 'candidate', 'active', 1402)",
                (),
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO workflow_effect_journal(
                     idempotency_key, identity_digest, identity_payload,
                     identity_payload_digest, prepared_payload, prepared_payload_digest,
                     operation, state, terminal_payload, terminal_payload_digest,
                     created_at, updated_at
                 ) VALUES (
                     'released-effect', 'released-identity', '{}', 'released-identity-payload',
                     '{}', 'released-prepared', 'released-operation', 'committed',
                     '{}', 'released-terminal', 1403, 1404
                 )",
                (),
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO workflow_handoffs(
                     token_digest, scope_payload, issued_at, expires_at, consumed,
                     frontier_payload, frontier_digest
                 ) VALUES ('released-handoff', '{}', 1405, 1406, 0, '{}', 'released-frontier')",
                (),
            )
            .unwrap();
    }

    connection
        .execute(
            "INSERT INTO workflow_runs(
                 run_id, parent_session_id, name, description, phase_json, status,
                 started_ts, ended_ts, result_summary, agent_count, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, 'completed', ?6, ?7, ?8, ?9, ?10, ?11)",
            params![
                RELEASED_FIXTURE_WORKFLOW_RUN_ID,
                RELEASED_FIXTURE_SESSION_ID,
                "released workflow",
                "released workflow fixture",
                "[]",
                1_410_i64,
                1_420_i64,
                "released result",
                1_i64,
                1_410_i64,
                1_420_i64,
            ],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO workflow_agents(
                 run_id, agent_label, agent_id, phase, transcript_path,
                 agent_session_id, status, model, tokens, started_ts, ended_ts,
                 created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'completed', ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                RELEASED_FIXTURE_WORKFLOW_RUN_ID,
                "released-agent",
                "released-agent-id",
                "run",
                "/released/profile/workflow.jsonl",
                RELEASED_FIXTURE_SESSION_ID,
                "gpt-5.6-codex",
                321_i64,
                1_411_i64,
                1_419_i64,
                1_411_i64,
                1_419_i64,
            ],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO workflow_index_meta(key, value, updated_at)
             VALUES ('released-watermark', 1420, 1421)",
            (),
        )
        .unwrap();

    connection
        .execute(
            "INSERT INTO git_correlation_meta(key, value, updated_at)
             VALUES ('released-git-watermark', 1430, 1431)",
            (),
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO git_evidence_publication_outbox(
                 receipt_id, publication_prefix, evidence_json, created_at
             ) VALUES ('released-git-receipt', 'released-prefix', '{}', 1432)",
            (),
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO git_history_index_progress(
                 activity_timestamp, source_rowid, provider, session_id,
                 project_path, window_start, window_end, worktree,
                 worktree_identity, git_dir, git_dir_identity, common_dir,
                 common_dir_identity, generation, scan_mode, reflog_path,
                 reflog_byte_offset, reflog_byte_length, source_generation,
                 reflog_digest, capture_target_offset, verify_byte_offset,
                 verify_digest, source_head_referent, source_head_oid,
                 cursor_head_state, cursor_head_branch, cursor_oid, segment_end,
                 segment_tip_oid, segment_cursor, emitted_count,
                 consulted_ref_seal_json
             ) VALUES (
                 1433, 1, 'codex', ?1, ?2, 1430, 1431,
                 X'776f726b74726565', X'776f726b747265652d6964',
                 X'6769742d646972', X'6769742d6469722d6964',
                 X'636f6d6d6f6e2d646972', X'636f6d6d6f6e2d6469722d6964',
                 0, 'reflog_capture', X'7265666c6f672d70617468', 0, 0,
                 'released-source-generation', 'released-reflog-digest', NULL, 0,
                 'sha256:ada855f318c248e40b2bb191bbe42fad3ec6300cc470ecca8d2e2322a6d82ae3',
                 NULL, 'released-head-oid', 'local_branch', 'released-main',
                 'released-cursor-oid', 1430, 'released-segment-tip', 0, 0, '{}'
             )",
            rusqlite::params![RELEASED_FIXTURE_SESSION_ID, "/released/profile/display",],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO git_history_index_segments(
                 source_rowid, ordinal, branch, start_ts, end_ts, tip_oid,
                 applied, completed
             ) VALUES (1, 0, 'released-main', 1430, 1431,
                       'released-segment-tip', 0, 0)",
            (),
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO git_history_index_pending(source_rowid, segment_ordinal, oid)
             VALUES (1, 0, ?1)",
            params![RELEASED_FIXTURE_GIT_OID],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO git_history_index_seen(source_rowid, segment_ordinal, oid)
             VALUES (1, 0, 'released-seen-oid')",
            (),
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO git_history_index_staged_spans(
                 source_rowid, segment_ordinal, boundary, branch, timestamp
             ) VALUES (1, 0, 0, 'released-main', 1434)",
            (),
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO git_history_index_staged_commits(
                 source_rowid, segment_ordinal, oid, branch, committed_at
             ) VALUES (1, 0, 'released-staged-oid', 'released-main', 1435)",
            (),
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO git_history_index_failures(
                 source_rowid, activity_timestamp, provider, session_id,
                 project_path, window_start, window_end, reason,
                 source_generation, reflog_digest
             ) VALUES (2, 1436, 'codex', ?1, ?2, 1430, 1431,
                       'unsupported_source_framing', NULL, NULL)",
            rusqlite::params![RELEASED_FIXTURE_SESSION_ID, "/released/profile/display",],
        )
        .unwrap();

    let (observation, cursor) =
        crate::schema_contract::invariants::test_fixture::authority_fixture(7, "released-row");
    let receipt = observation.receipt();
    let payload_digest = observation.payload_reference().digest().as_str().to_owned();
    let receipt_id = receipt.receipt().receipt_id().as_str().to_owned();
    connection
        .execute(
            "INSERT INTO sanitization_receipts(
                 receipt_id, sanitizer_version, payload_digest, receipt_json
             ) VALUES (?1, ?2, ?3, ?4)",
            params![
                receipt_id,
                receipt.receipt().sanitizer_version().as_str(),
                payload_digest,
                serde_json::to_string(receipt).unwrap(),
            ],
        )
        .unwrap();
    let observation_id = observation.observation_id().as_str().to_owned();
    connection
        .execute(
            "INSERT INTO observations(
                 observation_id, payload_digest, receipt_id,
                 observation_json, committed_cursor_json
             ) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                observation_id,
                observation.payload_reference().digest().as_str(),
                receipt.receipt().receipt_id().as_str(),
                serde_json::to_string(&observation).unwrap(),
                serde_json::to_string(&cursor).unwrap(),
            ],
        )
        .unwrap();
    connection
        .execute(
            "INSERT INTO source_cursors(source_json, scope_json, cursor_json)
             VALUES (?1, ?2, ?3)",
            params![
                serde_json::to_string(cursor.source()).unwrap(),
                serde_json::to_string(cursor.scope()).unwrap(),
                serde_json::to_string(&cursor).unwrap(),
            ],
        )
        .unwrap();

    ReleasedProfileFixture {
        observation_id,
        receipt_id,
    }
}
