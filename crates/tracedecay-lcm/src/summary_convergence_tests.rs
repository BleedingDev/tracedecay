use std::cell::Cell;

use tracedecay_runtime_core::db::engine::{
    Executor, IntoParams, QueryExecutor, Result as EngineResult, Rows, TestConnection, params,
};

use crate::{LCM_SCAN_PAGE_ROWS, LcmSourceRef, schema, summary_convergence};

const RELEASED_BETA37_FIXTURE: &str = include_str!("../tests/fixtures/lcm-released-beta37.sql");

/// Executor adapter that counts the statements a code path issues and the rows
/// those statements change, so write amplification is measured rather than
/// inferred.
struct CountingExecutor<'a> {
    inner: &'a TestConnection,
    queries: Cell<usize>,
    executes: Cell<usize>,
    rows_changed: Cell<u64>,
}

impl<'a> CountingExecutor<'a> {
    fn new(inner: &'a TestConnection) -> Self {
        Self {
            inner,
            queries: Cell::new(0),
            executes: Cell::new(0),
            rows_changed: Cell::new(0),
        }
    }
}

impl QueryExecutor for CountingExecutor<'_> {
    async fn query<P>(&self, sql: &str, params: P) -> EngineResult<Rows>
    where
        P: IntoParams,
    {
        self.queries.set(self.queries.get() + 1);
        self.inner.query(sql, params).await
    }
}

impl Executor for CountingExecutor<'_> {
    async fn execute<P>(&self, sql: &str, params: P) -> EngineResult<u64>
    where
        P: IntoParams,
    {
        self.executes.set(self.executes.get() + 1);
        let changed = self.inner.execute(sql, params).await?;
        self.rows_changed.set(self.rows_changed.get() + changed);
        Ok(changed)
    }

    async fn execute_batch(&self, sql: &str) -> EngineResult<()> {
        self.executes.set(self.executes.get() + 1);
        self.inner.execute_batch(sql).await
    }
}

async fn fetch_i64(conn: &TestConnection, sql: &str, params: impl IntoParams) -> i64 {
    let mut rows = conn.query(sql, params).await.unwrap();
    rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap()
}

#[tokio::test]
async fn backfill_page_upserts_each_session_once_and_idles_without_work() {
    const ROWS: i64 = 300;
    let temp = tempfile::tempdir().unwrap();
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
         INSERT INTO sessions(provider, session_id, project_key, project_path)
         VALUES ('cursor', 'session-a', 'project', '/p'),
                ('cursor', 'session-b', 'project', '/p'),
                ('cursor', 'session-c', 'project', '/p');",
    )
    .await
    .unwrap();
    schema::ensure_lcm_schema(&conn).await.unwrap();
    // Interleave three sessions so first-seen order (b, a, c) differs from
    // both lexical order and the order of the sessions table.
    let session_for = |ordinal: i64| match ordinal % 3 {
        1 => "session-b",
        2 => "session-a",
        _ => "session-c",
    };
    for ordinal in 1..=ROWS {
        conn.execute(
            "INSERT INTO lcm_raw_messages (
                provider, message_id, session_id, role, ordinal, content,
                content_hash, storage_kind, snippet_text, index_text, metadata_json
             ) VALUES ('cursor', ?1, ?2, 'assistant', ?3, 'body',
                       ?1, 'inline', 'body', 'body', '{}')",
            params![format!("message-{ordinal}"), session_for(ordinal), ordinal],
        )
        .await
        .unwrap();
    }
    // The insert trigger already queued every session. Model a store whose
    // rows predate the queue: drop two queue rows so the backfill must create
    // them, and give the third retry evidence the backfill must not erase.
    conn.execute_batch(
        "DELETE FROM lcm_summary_convergence_queue
         WHERE session_id IN ('session-a', 'session-b');
         UPDATE lcm_summary_convergence_queue
         SET state = 'retryable', failure_code = 'storage_unavailable',
             failure_count = 2, next_attempt_at_ms = 999
         WHERE session_id = 'session-c';",
    )
    .await
    .unwrap();

    assert!(
        summary_convergence::backfill_queue_has_work(&*conn)
            .await
            .unwrap()
    );

    let counting = CountingExecutor::new(&conn);
    let page = summary_convergence::backfill_queue_page(&counting, LCM_SCAN_PAGE_ROWS as usize)
        .await
        .unwrap();
    assert_eq!(page.rows_scanned, ROWS as usize);
    assert!(!page.has_more);
    assert!(
        counting.executes.get() < page.rows_scanned,
        "a {}-row page issued {} write statements (one per row again?)",
        page.rows_scanned,
        counting.executes.get()
    );

    // Queue rows: one per session, newest store id per session, and queue_id
    // (fair-scheduling order) in first-seen order for the rows the page
    // created, after the pre-existing row.
    let mut rows = conn
        .query(
            "SELECT session_id, newest_raw_store_id, state, failure_code, failure_count,
                    next_attempt_at_ms
             FROM lcm_summary_convergence_queue
             ORDER BY queue_id",
            (),
        )
        .await
        .unwrap();
    let mut queue = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        queue.push((
            row.get::<String>(0).unwrap(),
            row.get::<i64>(1).unwrap(),
            row.get::<String>(2).unwrap(),
            row.get::<Option<String>>(3).unwrap(),
            row.get::<i64>(4).unwrap(),
            row.get::<i64>(5).unwrap(),
        ));
    }
    drop(rows);
    assert_eq!(
        queue,
        vec![
            (
                "session-c".to_string(),
                300,
                "retryable".to_string(),
                Some("storage_unavailable".to_string()),
                2,
                999,
            ),
            (
                "session-b".to_string(),
                298,
                "pending".to_string(),
                None,
                0,
                0
            ),
            (
                "session-a".to_string(),
                299,
                "pending".to_string(),
                None,
                0,
                0
            ),
        ]
    );

    // Predecessor ranges: every message except each session's first keeps
    // its own exact predecessor interval.
    assert_eq!(
        fetch_i64(&conn, "SELECT COUNT(*) FROM lcm_raw_predecessor_ranges", ()).await,
        ROWS - 3
    );
    let mut range = conn
        .query(
            "SELECT session_id, from_store_id, to_store_id
             FROM lcm_raw_predecessor_ranges
             WHERE provider = 'cursor' AND message_id = 'message-7'",
            (),
        )
        .await
        .unwrap();
    let row = range.next().await.unwrap().unwrap();
    assert_eq!(row.get::<String>(0).unwrap(), "session-b");
    assert_eq!(row.get::<i64>(1).unwrap(), 1);
    assert_eq!(row.get::<i64>(2).unwrap(), 4);
    drop(range);
    assert_eq!(
        fetch_i64(
            &conn,
            "SELECT COUNT(*) FROM lcm_raw_predecessor_ranges WHERE message_id = 'message-1'",
            (),
        )
        .await,
        0,
        "a session's first message has no predecessor range"
    );

    // Everything is behind the frontier now: the read probe reports idle and
    // a page under the writer changes nothing.
    assert!(
        !summary_convergence::backfill_queue_has_work(&*conn)
            .await
            .unwrap()
    );
    let idle = CountingExecutor::new(&conn);
    let empty = summary_convergence::backfill_queue_page(&idle, LCM_SCAN_PAGE_ROWS as usize)
        .await
        .unwrap();
    assert_eq!(
        empty,
        summary_convergence::LcmSummaryQueueBackfillPage::default()
    );
    assert_eq!(idle.executes.get(), 0);
    assert_eq!(idle.rows_changed.get(), 0);

    // A new raw row past the frontier makes the probe report work again.
    conn.execute(
        "INSERT INTO lcm_raw_messages (
            provider, message_id, session_id, role, ordinal, content,
            content_hash, storage_kind, snippet_text, index_text, metadata_json
         ) VALUES ('cursor', 'message-301', 'session-a', 'assistant', 301, 'body',
                   'message-301', 'inline', 'body', 'body', '{}')",
        (),
    )
    .await
    .unwrap();
    assert!(
        summary_convergence::backfill_queue_has_work(&*conn)
            .await
            .unwrap()
    );
}

/// Raw fixture whose persisted ranges predate the policy-anchor role filter.
///
/// `first-user` owns a range it must lose (its only earlier row is an anchor)
/// and `compact-summary` owns an interval widened past the compact boundary.
async fn seed_preserved_role_filter_store(conn: &TestConnection) {
    create_session_host_tables(conn).await;
    conn.execute_batch(
        "INSERT INTO sessions(provider, session_id, project_key, project_path)
         VALUES ('claude', 'preserved', 'project.preserved', '/preserved');",
    )
    .await
    .unwrap();
    schema::ensure_lcm_schema(conn).await.unwrap();
    for (store_id, message_id, role) in [
        (1_i64, "session-open-system", "system"),
        (2, "first-user", "user"),
        (3, "compact_boundary:marker", "system"),
        (4, "compact-summary", "user"),
        (5, "reply", "assistant"),
        (6, "follow-up", "user"),
    ] {
        conn.execute(
            "INSERT INTO lcm_raw_messages (
                 store_id, provider, message_id, session_id, role, ordinal,
                 content, content_hash, storage_kind, snippet_text, index_text,
                 metadata_json
             ) VALUES (?1, 'claude', ?2, 'preserved', ?3, ?1, 'body', ?2,
                       'inline', 'body', 'body', '{}')",
            params![store_id, message_id, role],
        )
        .await
        .unwrap();
    }
    seed_pre_role_filter_ranges(conn).await;
}

/// The session tables the LCM schema's raw-identity triggers read. Store open
/// installs LCM objects beside them, so a store without them is not a shape
/// any profile presents.
async fn create_session_host_tables(conn: &TestConnection) {
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
         );",
    )
    .await
    .unwrap();
}

/// Ranges an ingest before the role filter would have written, and a journal
/// that has never recorded the rewrite: this is the store shape a preserved
/// profile presents on its first open under the role filter.
async fn seed_pre_role_filter_ranges(conn: &TestConnection) {
    conn.execute_batch(
        "INSERT INTO lcm_raw_predecessor_ranges (
             provider, message_id, session_id, from_store_id, to_store_id
         ) VALUES ('claude', 'first-user', 'preserved', 1, 1),
                  ('claude', 'compact-summary', 'preserved', 1, 3),
                  ('claude', 'follow-up', 'preserved', 1, 5);
         DELETE FROM lcm_gc_meta
         WHERE key = 'predecessor_range_role_filter_v1';",
    )
    .await
    .unwrap();
}

async fn predecessor_range(conn: &TestConnection, message_id: &str) -> Option<(i64, i64)> {
    let mut rows = conn
        .query(
            "SELECT from_store_id, to_store_id
             FROM lcm_raw_predecessor_ranges
             WHERE provider = 'claude' AND message_id = ?1",
            params![message_id],
        )
        .await
        .unwrap();
    rows.next()
        .await
        .unwrap()
        .map(|row| (row.get::<i64>(0).unwrap(), row.get::<i64>(1).unwrap()))
}

async fn journaled_rewrite_cursor(conn: &TestConnection) -> Option<String> {
    schema::get_gc_meta(
        &**conn,
        summary_convergence::PREDECESSOR_RANGE_ROLE_FILTER_KEY,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn role_filter_range_rewrite_pages_in_background_without_blocking_admission() {
    const PAGE_ROWS: usize = 2;
    let temp = tempfile::tempdir().unwrap();
    let conn = TestConnection::open(&temp.path().join("sessions.db"));
    seed_preserved_role_filter_store(&conn).await;

    // Admission: reopening an existing store must not perform the rewrite.
    schema::ensure_lcm_schema(&conn).await.unwrap();
    assert_eq!(
        predecessor_range(&conn, "compact-summary").await,
        Some((1, 3)),
        "store open must leave historical convergence to the background pass"
    );
    assert_eq!(
        journaled_rewrite_cursor(&conn).await,
        None,
        "store open must not journal rewrite progress"
    );
    assert!(
        summary_convergence::predecessor_range_rewrite_has_work(&*conn)
            .await
            .unwrap()
    );
    // Retrieval answers from the unrewritten store before the pass runs.
    assert_eq!(
        fetch_i64(
            &conn,
            "SELECT COUNT(*) FROM lcm_raw_messages WHERE session_id = 'preserved'",
            (),
        )
        .await,
        6
    );

    // First page covers store ids 1..=2 only: the range `first-user` must
    // lose is already gone while later stale rows are untouched.
    let first = summary_convergence::predecessor_range_rewrite_page(&*conn, PAGE_ROWS)
        .await
        .unwrap();
    assert_eq!(
        first,
        summary_convergence::LcmPredecessorRangeRewritePage {
            rows_rewritten: PAGE_ROWS,
            has_more: true,
        }
    );
    assert_eq!(
        predecessor_range(&conn, "first-user").await,
        None,
        "a row whose only predecessors are policy anchors must lose its range"
    );
    assert_eq!(
        predecessor_range(&conn, "compact-summary").await,
        Some((1, 3)),
        "a page must not rewrite rows above its keyset cursor"
    );
    assert_eq!(
        journaled_rewrite_cursor(&conn).await.as_deref(),
        Some("2"),
        "each page must journal its own keyset cursor"
    );

    // Restart mid-rewrite: a fresh pass resumes from the journaled cursor.
    drop(conn);
    let conn = TestConnection::open(&temp.path().join("sessions.db"));
    let resumed = summary_convergence::predecessor_range_rewrite_page(&*conn, PAGE_ROWS)
        .await
        .unwrap();
    assert_eq!(resumed.rows_rewritten, PAGE_ROWS);
    assert_eq!(
        predecessor_range(&conn, "compact-summary").await,
        Some((2, 2)),
        "the rewrite must narrow the interval to the conversational backlog"
    );

    let last = summary_convergence::predecessor_range_rewrite_page(&*conn, PAGE_ROWS)
        .await
        .unwrap();
    assert_eq!(last.rows_rewritten, PAGE_ROWS);
    assert_eq!(
        predecessor_range(&conn, "follow-up").await,
        Some((2, 5)),
        "the last page rewrites its own rows from the same authority"
    );
    assert!(
        summary_convergence::predecessor_range_rewrite_has_work(&*conn)
            .await
            .unwrap(),
        "the pass still owes its completion marker"
    );

    // The drained page retires the pass exactly once.
    let drained = summary_convergence::predecessor_range_rewrite_page(&*conn, PAGE_ROWS)
        .await
        .unwrap();
    assert_eq!(
        drained,
        summary_convergence::LcmPredecessorRangeRewritePage::default()
    );
    assert_eq!(
        journaled_rewrite_cursor(&conn).await.as_deref(),
        Some("applied")
    );
    assert!(
        !summary_convergence::predecessor_range_rewrite_has_work(&*conn)
            .await
            .unwrap()
    );
    conn.execute(
        "UPDATE lcm_raw_predecessor_ranges SET to_store_id = 3
         WHERE provider = 'claude' AND message_id = 'compact-summary'",
        (),
    )
    .await
    .unwrap();
    let after_marker = summary_convergence::predecessor_range_rewrite_page(&*conn, PAGE_ROWS)
        .await
        .unwrap();
    assert_eq!(
        after_marker,
        summary_convergence::LcmPredecessorRangeRewritePage::default()
    );
    assert_eq!(
        predecessor_range(&conn, "compact-summary").await,
        Some((2, 3)),
        "a completed rewrite must not run a second time"
    );
}

#[tokio::test]
async fn retained_queue_page_is_keyset_bounded_and_candidate_read_avoids_raw_corpus() {
    let temp = tempfile::tempdir().unwrap();
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
         INSERT INTO sessions(provider, session_id, project_key, project_path)
         VALUES ('cursor', 'large-corpus', 'project.large', '/large');",
    )
    .await
    .unwrap();
    schema::ensure_lcm_schema(&conn).await.unwrap();
    let transaction = conn
        .transaction_with_behavior(
            tracedecay_runtime_core::db::engine::TransactionBehavior::Immediate,
        )
        .await
        .unwrap();
    for ordinal in 1..=4_096_i64 {
        transaction
            .execute(
                "INSERT INTO lcm_raw_messages (
                    provider, message_id, session_id, role, ordinal, content,
                    content_hash, storage_kind, snippet_text, index_text, metadata_json
                 ) VALUES ('cursor', ?1, 'large-corpus', 'assistant', ?2, 'body',
                           ?1, 'inline', 'body', 'body', '{}')",
                params![format!("message-{ordinal}"), ordinal],
            )
            .await
            .unwrap();
    }
    transaction.commit().await.unwrap();

    let page = summary_convergence::backfill_queue_page(&*conn, LCM_SCAN_PAGE_ROWS as usize)
        .await
        .unwrap();
    assert_eq!(page.rows_scanned, LCM_SCAN_PAGE_ROWS as usize);
    assert!(page.has_more);
    let mut range_rows = conn
        .query(
            "SELECT COUNT(*) FROM lcm_raw_predecessor_ranges
             WHERE provider = 'cursor' AND session_id = 'large-corpus'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        range_rows
            .next()
            .await
            .unwrap()
            .unwrap()
            .get::<i64>(0)
            .unwrap(),
        LCM_SCAN_PAGE_ROWS - 1,
        "the bounded queue backfill must durably derive native predecessor ranges"
    );

    let mut plan = conn
        .query(
            &format!(
                "EXPLAIN QUERY PLAN {}",
                summary_convergence::NEXT_CANDIDATE_SQL
            ),
            params![i64::MAX],
        )
        .await
        .unwrap();
    let mut details = Vec::new();
    while let Some(row) = plan.next().await.unwrap() {
        details.push(row.get::<String>(3).unwrap());
    }
    assert!(
        details
            .iter()
            .any(|detail| detail.contains("idx_lcm_summary_convergence_due")),
        "candidate query did not use the due-work index: {details:?}"
    );
    assert!(
        details
            .iter()
            .all(|detail| !detail.contains("lcm_raw_messages")),
        "candidate query reached the raw corpus: {details:?}"
    );
    assert_eq!(
        summary_convergence::next_candidate(&*conn, i64::MAX)
            .await
            .unwrap()
            .unwrap()
            .session_id,
        "large-corpus"
    );
}

#[tokio::test]
async fn current_profiles_migrate_released_queue_predecessor_in_place() {
    let temp = tempfile::tempdir().unwrap();
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
         );",
    )
    .await
    .unwrap();
    schema::ensure_lcm_schema(&conn).await.unwrap();
    let version = schema::schema_version(&*conn).await.unwrap();
    conn.execute_batch(
        "DROP TRIGGER lcm_summary_convergence_raw_insert;
         DROP TRIGGER lcm_summary_convergence_raw_unprotected_update;
         DROP TRIGGER lcm_summary_convergence_dirty_raw_seed;
         DROP TABLE lcm_summary_convergence_invalidation_work;
         DROP TABLE lcm_summary_convergence_dirty_raw;
         DROP TABLE lcm_summary_convergence_queue;
         CREATE TABLE lcm_summary_convergence_dirty_raw (
            provider TEXT NOT NULL,
            session_id TEXT NOT NULL,
            store_id INTEGER NOT NULL,
            PRIMARY KEY(provider, session_id, store_id),
            FOREIGN KEY(provider, session_id)
                REFERENCES sessions(provider, session_id) ON DELETE CASCADE
         );
         CREATE TABLE lcm_summary_convergence_invalidation_work (
            provider TEXT NOT NULL,
            session_id TEXT NOT NULL,
            raw_store_id INTEGER NOT NULL,
            source_kind TEXT NOT NULL,
            source_id TEXT NOT NULL,
            depth INTEGER NOT NULL,
            after_node_id TEXT NOT NULL DEFAULT '',
            PRIMARY KEY(provider, session_id, raw_store_id, source_kind, source_id),
            FOREIGN KEY(provider, session_id, raw_store_id)
                REFERENCES lcm_summary_convergence_dirty_raw(provider, session_id, store_id)
                ON DELETE CASCADE
         );
         CREATE TABLE lcm_summary_convergence_queue (
            queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider TEXT NOT NULL,
            session_id TEXT NOT NULL,
            newest_raw_store_id INTEGER NOT NULL,
            protection_frontier_store_id INTEGER NOT NULL DEFAULT 0,
            attempted_raw_store_id INTEGER NOT NULL DEFAULT 0,
            state TEXT NOT NULL DEFAULT 'pending'
                CHECK(state IN ('pending', 'retryable', 'current', 'unavailable', 'permanent')),
            failure_code TEXT,
            failure_count INTEGER NOT NULL DEFAULT 0,
            next_attempt_at_ms INTEGER NOT NULL DEFAULT 0,
            attempt_generation INTEGER NOT NULL DEFAULT 0,
            UNIQUE(provider, session_id),
            FOREIGN KEY(provider, session_id)
                REFERENCES sessions(provider, session_id) ON DELETE CASCADE
         );",
    )
    .await
    .unwrap();
    conn.execute_batch(
        "INSERT INTO sessions(provider, session_id, project_key, project_path)
         VALUES ('cursor', 'legacy-shape', 'project.legacy', '/legacy');
         INSERT INTO lcm_summary_convergence_queue(
             queue_id, provider, session_id, newest_raw_store_id,
             protection_frontier_store_id, attempted_raw_store_id, state,
             failure_code, failure_count, next_attempt_at_ms, attempt_generation
         ) VALUES (42, 'cursor', 'legacy-shape', 9, 8, 7, 'retryable',
                   'legacy_provider_busy', 3, 1234, 5);
         INSERT INTO lcm_summary_convergence_dirty_raw(
             provider, session_id, store_id
         ) VALUES ('cursor', 'legacy-shape', 9);
         INSERT INTO lcm_summary_convergence_invalidation_work(
             provider, session_id, raw_store_id, source_kind, source_id,
             depth, after_node_id
         ) VALUES ('cursor', 'legacy-shape', 9, 'raw_message', '9', 0, '');",
    )
    .await
    .unwrap();

    schema::ensure_lcm_schema(&conn).await.unwrap();

    assert_eq!(schema::schema_version(&*conn).await.unwrap(), version);
    assert_eq!(
        schema::require_admissible_lcm_schema(&*conn).await.unwrap(),
        schema::LcmSchemaAdmission::Current
    );
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM sqlite_schema
             WHERE type = 'table' AND name = 'lcm_summary_convergence_queue'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        1
    );
    for column in ["raw_revision_generation", "stale_from_store_id"] {
        let mut rows = conn
            .query(
                "SELECT COUNT(*) FROM pragma_table_info('lcm_summary_convergence_queue')
                 WHERE name = ?1",
                params![column],
            )
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            1,
            "missing in-place queue column {column}"
        );
    }
    let mut rows = conn
        .query(
            "SELECT COUNT(*) FROM sqlite_schema
             WHERE type = 'table' AND name = 'lcm_summary_convergence_dirty_raw'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        1
    );
    let mut rows = conn
        .query(
            "SELECT COUNT(*)
             FROM pragma_table_info('lcm_summary_convergence_dirty_raw')
             WHERE name = 'rewind_frontier_store_id'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        1,
        "missing durable invalidation rewind frontier"
    );
    for object in [
        "lcm_summary_convergence_invalidation_work",
        "lcm_summary_convergence_dirty_raw_seed",
        "idx_lcm_summary_sources_source_node",
    ] {
        let mut rows = conn
            .query(
                "SELECT COUNT(*) FROM sqlite_schema WHERE name = ?1",
                params![object],
            )
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            1,
            "missing in-place invalidation authority {object}"
        );
    }
    let mut rows = conn
        .query(
            "SELECT COUNT(*)
             FROM pragma_table_info('lcm_summary_convergence_invalidation_work')
             WHERE name = 'state'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        1,
        "missing durable invalidation visited state"
    );
    let mut rows = conn
        .query(
            "SELECT queue_id, provider, session_id, newest_raw_store_id,
                    protection_frontier_store_id, attempted_raw_store_id, state,
                    failure_code, failure_count, next_attempt_at_ms,
                    attempt_generation, raw_revision_generation, stale_from_store_id
             FROM lcm_summary_convergence_queue
             WHERE provider = 'cursor' AND session_id = 'legacy-shape'",
            (),
        )
        .await
        .unwrap();
    let queue_row = rows.next().await.unwrap().unwrap();
    assert_eq!(queue_row.get::<i64>(0).unwrap(), 42);
    assert_eq!(queue_row.get::<String>(1).unwrap(), "cursor");
    assert_eq!(queue_row.get::<String>(2).unwrap(), "legacy-shape");
    assert_eq!(queue_row.get::<i64>(3).unwrap(), 9);
    assert_eq!(queue_row.get::<i64>(4).unwrap(), 8);
    assert_eq!(queue_row.get::<i64>(5).unwrap(), 7);
    assert_eq!(queue_row.get::<String>(6).unwrap(), "retryable");
    assert_eq!(
        queue_row.get::<Option<String>>(7).unwrap().as_deref(),
        Some("legacy_provider_busy")
    );
    assert_eq!(queue_row.get::<i64>(8).unwrap(), 3);
    assert_eq!(queue_row.get::<i64>(9).unwrap(), 1234);
    assert_eq!(queue_row.get::<i64>(10).unwrap(), 5);
    assert_eq!(queue_row.get::<i64>(11).unwrap(), 0);
    assert_eq!(queue_row.get::<Option<i64>>(12).unwrap(), None);
    drop(rows);
    let mut rows = conn
        .query(
            "SELECT provider, session_id, store_id, rewind_frontier_store_id
             FROM lcm_summary_convergence_dirty_raw
             WHERE provider = 'cursor' AND session_id = 'legacy-shape' AND store_id = 9",
            (),
        )
        .await
        .unwrap();
    let dirty_row = rows.next().await.unwrap().unwrap();
    assert_eq!(dirty_row.get::<String>(0).unwrap(), "cursor");
    assert_eq!(dirty_row.get::<String>(1).unwrap(), "legacy-shape");
    assert_eq!(dirty_row.get::<i64>(2).unwrap(), 9);
    assert_eq!(dirty_row.get::<i64>(3).unwrap(), 8);
    drop(rows);
    let mut rows = conn
        .query(
            "SELECT provider, session_id, raw_store_id, source_kind, source_id,
                    depth, after_node_id, state
             FROM lcm_summary_convergence_invalidation_work
             WHERE provider = 'cursor' AND session_id = 'legacy-shape' AND raw_store_id = 9",
            (),
        )
        .await
        .unwrap();
    let invalidation_row = rows.next().await.unwrap().unwrap();
    assert_eq!(invalidation_row.get::<String>(0).unwrap(), "cursor");
    assert_eq!(invalidation_row.get::<String>(1).unwrap(), "legacy-shape");
    assert_eq!(invalidation_row.get::<i64>(2).unwrap(), 9);
    assert_eq!(invalidation_row.get::<String>(3).unwrap(), "raw_message");
    assert_eq!(invalidation_row.get::<String>(4).unwrap(), "9");
    assert_eq!(invalidation_row.get::<i64>(5).unwrap(), 0);
    assert_eq!(invalidation_row.get::<String>(6).unwrap(), "");
    assert_eq!(invalidation_row.get::<String>(7).unwrap(), "pending");
    drop(rows);
    assert_eq!(
        fetch_i64(&conn, "SELECT COUNT(*) FROM lcm_raw_predecessor_ranges", (),).await,
        0
    );
}

#[tokio::test]
async fn current_admission_rejects_queue_shape_without_released_state_check() {
    let temp = tempfile::tempdir().unwrap();
    let conn = TestConnection::open(&temp.path().join("sessions.db"));
    create_session_host_tables(&conn).await;
    schema::ensure_lcm_schema(&conn).await.unwrap();
    conn.execute_batch(
        "DROP TRIGGER lcm_summary_convergence_raw_insert;
         DROP TRIGGER lcm_summary_convergence_raw_unprotected_update;
         DROP TABLE lcm_summary_convergence_queue;
         CREATE TABLE lcm_summary_convergence_queue (
            queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
            provider TEXT NOT NULL,
            session_id TEXT NOT NULL,
            newest_raw_store_id INTEGER NOT NULL,
            protection_frontier_store_id INTEGER NOT NULL DEFAULT 0,
            attempted_raw_store_id INTEGER NOT NULL DEFAULT 0,
            state TEXT NOT NULL DEFAULT 'pending',
            failure_code TEXT,
            failure_count INTEGER NOT NULL DEFAULT 0,
            next_attempt_at_ms INTEGER NOT NULL DEFAULT 0,
            attempt_generation INTEGER NOT NULL DEFAULT 0,
            UNIQUE(provider, session_id),
            FOREIGN KEY(provider, session_id)
                REFERENCES sessions(provider, session_id) ON DELETE CASCADE
         );",
    )
    .await
    .unwrap();

    let error = schema::require_admissible_lcm_schema(&*conn)
        .await
        .expect_err("an unknown queue predecessor must require reset");
    assert!(matches!(
        error,
        crate::LcmError::ProfileResetRequired { .. }
    ));
}

#[tokio::test]
async fn protected_content_revision_requeues_a_current_session() {
    let temp = tempfile::tempdir().unwrap();
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
         INSERT INTO sessions(provider, session_id, project_key, project_path)
         VALUES ('cursor', 'revised-session', 'project.revised', '/revised');",
    )
    .await
    .unwrap();
    schema::ensure_lcm_schema(&conn).await.unwrap();
    conn.execute(
        "INSERT INTO lcm_raw_messages (
            provider, message_id, session_id, role, ordinal, content,
            content_hash, storage_kind, snippet_text, index_text, metadata_json
         ) VALUES ('cursor', 'message-1', 'revised-session', 'assistant', 1,
                   'old content', 'old-hash', 'inline', 'old content', 'old content',
                   '{\"ingest_protection\":{\"sanitization_receipt\":{}}}')",
        (),
    )
    .await
    .unwrap();
    let candidate = summary_convergence::next_candidate(&conn, i64::MAX)
        .await
        .unwrap()
        .unwrap();
    assert!(
        summary_convergence::record_outcome(
            &conn,
            &candidate,
            summary_convergence::LcmSummaryConvergenceQueueState::Current,
            None,
            0,
            0,
        )
        .await
        .unwrap()
    );
    assert!(
        summary_convergence::next_candidate(&conn, i64::MAX)
            .await
            .unwrap()
            .is_none()
    );

    conn.execute(
        "UPDATE lcm_raw_messages
         SET content = 'revised content', content_hash = 'revised-hash',
             snippet_text = 'revised content', index_text = 'revised content'
         WHERE provider = 'cursor' AND message_id = 'message-1'",
        (),
    )
    .await
    .unwrap();

    let revised = summary_convergence::next_candidate(&conn, i64::MAX)
        .await
        .unwrap()
        .expect("same-store content revisions must become due work");
    assert_eq!(revised.session_id, "revised-session");
    assert!(revised.attempted_raw_store_id < revised.newest_raw_store_id);
    assert!(
        !summary_convergence::record_outcome(
            &conn,
            &candidate,
            summary_convergence::LcmSummaryConvergenceQueueState::Current,
            None,
            0,
            0,
        )
        .await
        .unwrap(),
        "an outcome from the superseded raw generation must lose its CAS"
    );
}

#[tokio::test]
async fn protection_progress_cannot_overwrite_a_concurrent_raw_rewind() {
    let temp = tempfile::tempdir().unwrap();
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
         INSERT INTO sessions(provider, session_id, project_key, project_path)
         VALUES ('cursor', 'protection-cas', 'project.cas', '/cas');",
    )
    .await
    .unwrap();
    schema::ensure_lcm_schema(&conn).await.unwrap();
    conn.execute(
        "INSERT INTO lcm_raw_messages (
            provider, message_id, session_id, role, ordinal, content,
            content_hash, storage_kind, snippet_text, index_text, metadata_json
         ) VALUES ('cursor', 'message-1', 'protection-cas', 'assistant', 1,
                   'old', 'old-hash', 'inline', 'old', 'old',
                   '{\"ingest_protection\":{\"sanitization_receipt\":{}}}')",
        (),
    )
    .await
    .unwrap();
    let candidate = summary_convergence::next_candidate(&conn, i64::MAX)
        .await
        .unwrap()
        .unwrap();

    conn.execute(
        "UPDATE lcm_raw_messages SET content_hash = 'new-hash'
         WHERE provider = 'cursor' AND message_id = 'message-1'",
        (),
    )
    .await
    .unwrap();
    let error = summary_convergence::record_current_protection_progress(
        &conn,
        "cursor",
        "protection-cas",
        999,
        candidate.raw_revision_generation,
    )
    .await
    .unwrap_err();
    assert!(matches!(error, crate::LcmError::StaleRawRevision { .. }));
    let refreshed = summary_convergence::candidate_for_session(&conn, "cursor", "protection-cas")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(refreshed.protection_frontier_store_id, 0);
}

#[tokio::test]
async fn disjoint_raw_revisions_drain_as_distinct_restart_safe_work_items() {
    let temp = tempfile::tempdir().unwrap();
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
         INSERT INTO sessions(provider, session_id, project_key, project_path)
         VALUES ('cursor', 'disjoint-revisions', 'project.revised', '/revised');",
    )
    .await
    .unwrap();
    schema::ensure_lcm_schema(&conn).await.unwrap();
    for ordinal in 1..=2 {
        conn.execute(
            "INSERT INTO lcm_raw_messages (
                provider, message_id, session_id, role, ordinal, content,
                content_hash, storage_kind, snippet_text, index_text, metadata_json
             ) VALUES ('cursor', ?1, 'disjoint-revisions', 'assistant', ?2,
                       ?1, ?1, 'inline', ?1, ?1,
                       '{\"ingest_protection\":{\"sanitization_receipt\":{}}}')",
            params![format!("message-{ordinal}"), ordinal],
        )
        .await
        .unwrap();
    }
    let current = summary_convergence::next_candidate(&conn, i64::MAX)
        .await
        .unwrap()
        .unwrap();
    assert!(
        summary_convergence::record_outcome(
            &conn,
            &current,
            summary_convergence::LcmSummaryConvergenceQueueState::Current,
            None,
            0,
            0,
        )
        .await
        .unwrap()
    );
    conn.execute(
        "UPDATE lcm_raw_messages SET ordinal = 101
         WHERE provider = 'cursor' AND message_id = 'message-1'",
        (),
    )
    .await
    .unwrap();
    conn.execute(
        "UPDATE lcm_raw_messages SET content_hash = 'revised-2'
         WHERE provider = 'cursor' AND message_id = 'message-2'",
        (),
    )
    .await
    .unwrap();
    let first = summary_convergence::next_candidate(&conn, i64::MAX)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.stale_from_store_id, Some(1));
    assert!(
        summary_convergence::complete_stale_raw_revision(&conn, &first)
            .await
            .unwrap()
    );
    let after_restart =
        summary_convergence::candidate_for_session(&conn, "cursor", "disjoint-revisions")
            .await
            .unwrap()
            .unwrap();
    assert_eq!(after_restart.stale_from_store_id, Some(2));
    assert!(
        summary_convergence::complete_stale_raw_revision(&conn, &after_restart)
            .await
            .unwrap()
    );
    let drained = summary_convergence::candidate_for_session(&conn, "cursor", "disjoint-revisions")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(drained.stale_from_store_id, None);
    assert!(
        summary_convergence::record_outcome(
            &conn,
            &drained,
            summary_convergence::LcmSummaryConvergenceQueueState::Current,
            None,
            0,
            0,
        )
        .await
        .unwrap()
    );
    assert!(
        summary_convergence::next_candidate(&conn, i64::MAX)
            .await
            .unwrap()
            .is_none()
    );
}

/// A store created after the role-aware filter owes no rewrite: ingest writes
/// every interval under the current filter, so paging the corpus to re-derive
/// them would be pure waste.
#[tokio::test]
async fn a_fresh_store_opens_with_the_range_rewrite_already_retired() {
    let temp = tempfile::tempdir().unwrap();
    let conn = TestConnection::open(&temp.path().join("sessions.db"));
    create_session_host_tables(&conn).await;

    schema::ensure_lcm_schema(&conn).await.unwrap();

    assert_eq!(
        journaled_rewrite_cursor(&conn).await.as_deref(),
        Some("applied"),
        "a fresh install must journal the rewrite as already retired"
    );
    assert!(
        !summary_convergence::predecessor_range_rewrite_has_work(&*conn)
            .await
            .unwrap(),
        "a fresh store must not hand the background worker a corpus-wide pass"
    );

    // Reopening preserves the retirement rather than re-arming the pass.
    schema::ensure_lcm_schema(&conn).await.unwrap();
    assert!(
        !summary_convergence::predecessor_range_rewrite_has_work(&*conn)
            .await
            .unwrap()
    );
}

/// Exercise the durable work owned by the post-beta.37 migration against a
/// released, rowful store.  Queue retries, raw revisions and predecessor
/// ranges must all remain restart-safe when the source rows already exist.
#[tokio::test]
async fn released_beta37_rows_materialize_retry_invalidation_and_ranges() {
    let temp = tempfile::tempdir().unwrap();
    let conn = TestConnection::open(&temp.path().join("sessions.db"));
    conn.execute_batch(RELEASED_BETA37_FIXTURE).await.unwrap();

    schema::ensure_lcm_schema(&conn).await.unwrap();
    let page = summary_convergence::backfill_queue_page(&*conn, 128)
        .await
        .unwrap();
    assert_eq!(page.rows_scanned, 4);
    assert!(!page.has_more);

    // Hydration follows the persisted source closure: raw source ids are
    // numeric store ids, and the canonical hashes/receipts make both rows
    // verifiable instead of leaving this fixture as metadata-only evidence.
    let expansions = crate::dag::expand_summary_nodes(
        &*conn,
        "cursor",
        "beta37-session-a",
        &[
            "summary-beta37-root".to_owned(),
            "summary-beta37-leaf".to_owned(),
        ],
    )
    .await
    .unwrap();
    assert_eq!(expansions.len(), 2);
    assert_eq!(
        &expansions[0].sources[0].source_ref,
        &LcmSourceRef::RawMessage { store_id: 10 }
    );
    assert_eq!(
        &expansions[1].sources[1].source_ref,
        &LcmSourceRef::RawMessage { store_id: 11 }
    );

    let candidate =
        summary_convergence::candidate_for_session(&*conn, "cursor", "beta37-session-a")
            .await
            .unwrap()
            .expect("beta37 cursor rows must be queued");
    assert!(
        summary_convergence::record_outcome(
            &*conn,
            &candidate,
            summary_convergence::LcmSummaryConvergenceQueueState::Retryable,
            Some("fixture_provider_busy"),
            2,
            9_999,
        )
        .await
        .unwrap()
    );
    let mut queue = conn
        .query(
            "SELECT state, failure_code, failure_count, next_attempt_at_ms
             FROM lcm_summary_convergence_queue
             WHERE provider = 'cursor' AND session_id = 'beta37-session-a'",
            (),
        )
        .await
        .unwrap();
    let queue_row = queue.next().await.unwrap().unwrap();
    assert_eq!(queue_row.get::<String>(0).unwrap(), "retryable");
    assert_eq!(
        queue_row.get::<Option<String>>(1).unwrap().as_deref(),
        Some("fixture_provider_busy")
    );
    assert_eq!(queue_row.get::<i64>(2).unwrap(), 2);
    assert_eq!(queue_row.get::<i64>(3).unwrap(), 9_999);
    drop(queue);

    // A persisted raw revision creates both dirty work and its seed row.  A
    // later worker restart can therefore discover the invalidation without
    // relying on process-local state.
    conn.execute(
        "UPDATE lcm_raw_messages
         SET metadata_json = json_set(metadata_json, '$.fixture_revision', json('true'))
         WHERE provider = 'cursor' AND message_id = 'beta37-message-a1'",
        (),
    )
    .await
    .unwrap();
    assert_eq!(
        fetch_i64(
            &conn,
            "SELECT COUNT(*) FROM lcm_summary_convergence_dirty_raw
             WHERE provider = 'cursor' AND session_id = 'beta37-session-a'",
            (),
        )
        .await,
        1
    );
    assert_eq!(
        fetch_i64(
            &conn,
            "SELECT COUNT(*) FROM lcm_summary_convergence_invalidation_work
             WHERE provider = 'cursor' AND session_id = 'beta37-session-a'
               AND raw_store_id = 10 AND source_kind = 'raw_message'",
            (),
        )
        .await,
        1
    );

    // Walk the actual persisted invalidation closure from the dirty raw seed
    // through raw-message and summary-node source edges.  This catches a
    // fixture that merely has a seed row while using non-addressable message
    // ids: store 10 must invalidate the root and its dependent leaf.
    let mut invalidation = conn
        .query(
            "WITH RECURSIVE invalidated(raw_store_id, node_id, depth) AS (
                 SELECT work.raw_store_id, source.node_id, 0
                 FROM lcm_summary_convergence_invalidation_work work
                 JOIN lcm_summary_sources source
                   ON source.source_kind = 'raw_message'
                  AND CAST(source.source_id AS INTEGER) = work.raw_store_id
                 WHERE work.provider = ?1
                   AND work.session_id = ?2
                   AND work.state = 'pending'
                 UNION
                 SELECT invalidated.raw_store_id, source.node_id,
                        invalidated.depth + 1
                 FROM invalidated
                 JOIN lcm_summary_sources source
                   ON source.source_kind = 'summary_node'
                  AND source.source_id = invalidated.node_id
             )
             SELECT node_id, depth
             FROM invalidated
             WHERE raw_store_id = 10
             ORDER BY depth, node_id",
            params!["cursor", "beta37-session-a"],
        )
        .await
        .unwrap();
    let mut invalidated_nodes = Vec::new();
    while let Some(row) = invalidation.next().await.unwrap() {
        invalidated_nodes.push((row.get::<String>(0).unwrap(), row.get::<i64>(1).unwrap()));
    }
    assert_eq!(
        invalidated_nodes,
        vec![
            ("summary-beta37-root".to_owned(), 0),
            ("summary-beta37-leaf".to_owned(), 1),
        ]
    );

    // The migration's bounded backfill derives the exact conversational
    // predecessor interval for the two retained sessions.
    assert_eq!(
        fetch_i64(&conn, "SELECT COUNT(*) FROM lcm_raw_predecessor_ranges", ()).await,
        2
    );
    let mut range = conn
        .query(
            "SELECT from_store_id, to_store_id
             FROM lcm_raw_predecessor_ranges
             WHERE provider = 'cursor' AND message_id = 'beta37-message-a2'",
            (),
        )
        .await
        .unwrap();
    let range_row = range.next().await.unwrap().unwrap();
    assert_eq!(range_row.get::<i64>(0).unwrap(), 10);
    assert_eq!(range_row.get::<i64>(1).unwrap(), 10);
    drop(range);

    // FTS remains live after migration: updating the content column changes
    // the searchable projection through the canonical trigger.
    conn.execute(
        "UPDATE lcm_raw_messages
         SET index_text = 'beta37 migration fts revision'
         WHERE provider = 'cursor' AND message_id = 'beta37-message-a1'",
        (),
    )
    .await
    .unwrap();
    assert_eq!(
        fetch_i64(
            &conn,
            "SELECT COUNT(*) FROM lcm_raw_messages_fts
             WHERE lcm_raw_messages_fts MATCH 'migration'",
            (),
        )
        .await,
        1
    );
    assert_eq!(
        fetch_i64(
            &conn,
            "SELECT COUNT(*) FROM lcm_summary_nodes_fts
             WHERE lcm_summary_nodes_fts MATCH 'retains'",
            (),
        )
        .await,
        2
    );
}

#[tokio::test]
async fn convergence_schema_refuses_malformed_queue_and_seed_before_mutation() {
    let temp = tempfile::tempdir().unwrap();
    let conn = TestConnection::open(&temp.path().join("sessions.db"));
    create_session_host_tables(&conn).await;
    schema::ensure_lcm_schema(&conn).await.unwrap();

    // Keep the same object name and columns so CREATE IF NOT EXISTS cannot
    // repair the weakened uniqueness constraint.  Admission must stop before it
    // executes any of the ALTER/CREATE work below.
    conn.execute_batch(
        "DROP TRIGGER lcm_summary_convergence_raw_insert;
         DROP TRIGGER lcm_summary_convergence_raw_unprotected_update;
         DROP INDEX idx_lcm_summary_convergence_due;
         DROP TABLE lcm_summary_convergence_queue;
         CREATE TABLE lcm_summary_convergence_queue (
             queue_id INTEGER PRIMARY KEY AUTOINCREMENT,
             provider TEXT NOT NULL,
             session_id TEXT NOT NULL,
             newest_raw_store_id INTEGER NOT NULL,
             protection_frontier_store_id INTEGER NOT NULL DEFAULT 0,
             attempted_raw_store_id INTEGER NOT NULL DEFAULT 0,
             state TEXT NOT NULL DEFAULT 'pending'
                 CHECK(state IN ('pending', 'retryable', 'current', 'unavailable', 'permanent')),
             failure_code TEXT,
             failure_count INTEGER NOT NULL DEFAULT 0,
             next_attempt_at_ms INTEGER NOT NULL DEFAULT 0,
             attempt_generation INTEGER NOT NULL DEFAULT 0,
             raw_revision_generation INTEGER NOT NULL DEFAULT 0,
             stale_from_store_id INTEGER,
             FOREIGN KEY(provider, session_id)
                 REFERENCES sessions(provider, session_id) ON DELETE CASCADE
         );",
    )
    .await
    .unwrap();
    let malformed_queue_sql = {
        let mut rows = conn
            .query(
                "SELECT sql FROM sqlite_master
                 WHERE name = 'lcm_summary_convergence_queue'",
                (),
            )
            .await
            .unwrap();
        rows.next()
            .await
            .unwrap()
            .unwrap()
            .get::<String>(0)
            .unwrap()
    };
    let error = summary_convergence::ensure_schema(&*conn)
        .await
        .expect_err("a weakened queue UNIQUE constraint must fail closed");
    assert!(matches!(error, crate::LcmError::Db(_)));
    let mut rows = conn
        .query(
            "SELECT sql FROM sqlite_master
             WHERE name = 'lcm_summary_convergence_queue'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next()
            .await
            .unwrap()
            .unwrap()
            .get::<String>(0)
            .unwrap(),
        malformed_queue_sql
    );

    // An inert same-name seed trigger is equally dangerous: IF NOT EXISTS
    // would preserve it, so the preflight compares its complete body.
    let temp = tempfile::tempdir().unwrap();
    let conn = TestConnection::open(&temp.path().join("sessions.db"));
    create_session_host_tables(&conn).await;
    schema::ensure_lcm_schema(&conn).await.unwrap();
    conn.execute_batch(
        "DROP TRIGGER lcm_summary_convergence_dirty_raw_seed;
         CREATE TRIGGER lcm_summary_convergence_dirty_raw_seed
             AFTER INSERT ON lcm_summary_convergence_dirty_raw WHEN 0 BEGIN
                 SELECT 1;
             END;",
    )
    .await
    .unwrap();
    let mut rows = conn
        .query(
            "SELECT sql FROM sqlite_master
             WHERE name = 'lcm_summary_convergence_dirty_raw_seed'",
            (),
        )
        .await
        .unwrap();
    let inert_seed_sql = rows
        .next()
        .await
        .unwrap()
        .unwrap()
        .get::<String>(0)
        .unwrap();
    let error = summary_convergence::ensure_schema(&*conn)
        .await
        .expect_err("an inert seed trigger must fail closed");
    assert!(matches!(error, crate::LcmError::Db(_)));
    let mut rows = conn
        .query(
            "SELECT sql FROM sqlite_master
             WHERE name = 'lcm_summary_convergence_dirty_raw_seed'",
            (),
        )
        .await
        .unwrap();
    assert_eq!(
        rows.next()
            .await
            .unwrap()
            .unwrap()
            .get::<String>(0)
            .unwrap(),
        inert_seed_sql
    );
}
