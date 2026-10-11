//! Measurement helpers shared by this crate's unit tests.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tracedecay_runtime_core::db::engine::{
    Error as EngineError, Executor, IntoParams, QueryExecutor, Result as EngineResult, Rows,
};

use crate::handle::{SessionTemporalExec, SessionTemporalQuery};

/// Connection adapter that counts the read statements a code path issues, so
/// round-trip amplification is measured rather than inferred.
pub(crate) struct QueryCountingConnection<'a, T> {
    inner: &'a T,
    queries: AtomicUsize,
}

impl<'a, T> QueryCountingConnection<'a, T> {
    pub(crate) fn new(inner: &'a T) -> Self {
        Self {
            inner,
            queries: AtomicUsize::new(0),
        }
    }

    pub(crate) fn query_count(&self) -> usize {
        self.queries.load(Ordering::Relaxed)
    }
}

impl<T: QueryExecutor> QueryExecutor for QueryCountingConnection<'_, T> {
    async fn query<P>(&self, sql: &str, params: P) -> EngineResult<Rows>
    where
        P: IntoParams,
    {
        self.queries.fetch_add(1, Ordering::Relaxed);
        self.inner.query(sql, params).await
    }
}

impl<T: Executor> Executor for QueryCountingConnection<'_, T> {
    async fn execute<P>(&self, sql: &str, params: P) -> EngineResult<u64>
    where
        P: IntoParams,
    {
        self.inner.execute(sql, params).await
    }

    async fn execute_batch(&self, sql: &str) -> EngineResult<()> {
        self.inner.execute_batch(sql).await
    }
}

impl<T: SessionTemporalQuery> SessionTemporalQuery for QueryCountingConnection<'_, T> {
    fn query<P>(
        &self,
        sql: &str,
        params: P,
    ) -> impl Future<Output = Result<Rows, EngineError>> + Send
    where
        P: IntoParams + Send,
    {
        self.queries.fetch_add(1, Ordering::Relaxed);
        SessionTemporalQuery::query(self.inner, sql, params)
    }
}

impl<T: SessionTemporalExec> SessionTemporalExec for QueryCountingConnection<'_, T> {
    fn execute<P>(
        &self,
        sql: &str,
        params: P,
    ) -> impl Future<Output = Result<u64, EngineError>> + Send
    where
        P: IntoParams + Send,
    {
        SessionTemporalExec::execute(self.inner, sql, params)
    }

    fn execute_batch(&self, sql: &str) -> impl Future<Output = Result<(), EngineError>> + Send {
        SessionTemporalExec::execute_batch(self.inner, sql)
    }
}

/// Registered-database wrapper that fails the Nth `execute` whose SQL
/// starts with `prefix`. The real maintained transaction still runs
/// underneath; only that boundary statement errors so grouped-commit
/// abort paths are covered through the production authority.
pub(crate) struct FailingRegisteredDb<'a> {
    inner: &'a tracedecay_global_db::RegisteredGlobalDb,
    prefix: &'static str,
    fail_on_match: usize,
    matches: Arc<AtomicUsize>,
}

impl<'a> FailingRegisteredDb<'a> {
    pub(crate) fn fail_nth_prefixed_execute(
        inner: &'a tracedecay_global_db::RegisteredGlobalDb,
        prefix: &'static str,
        fail_on_match: usize,
    ) -> Self {
        Self {
            inner,
            prefix,
            fail_on_match,
            matches: Arc::new(AtomicUsize::new(0)),
        }
    }
}

/// The write transaction `FailingRegisteredDb` opens: delegates every call
/// except `execute`, which reports a runtime error on the chosen match.
pub(crate) struct FailingWriteTxn<'a> {
    inner: tracedecay_global_db::RegisteredGlobalDbWriteTransaction<'a>,
    prefix: &'static str,
    fail_on_match: usize,
    matches: Arc<AtomicUsize>,
}

impl SessionTemporalQuery for FailingWriteTxn<'_> {
    fn query<P>(
        &self,
        sql: &str,
        params: P,
    ) -> impl Future<Output = Result<Rows, EngineError>> + Send
    where
        P: IntoParams + Send,
    {
        SessionTemporalQuery::query(&self.inner, sql, params)
    }
}

impl SessionTemporalExec for FailingWriteTxn<'_> {
    fn execute<P>(
        &self,
        sql: &str,
        params: P,
    ) -> impl Future<Output = Result<u64, EngineError>> + Send
    where
        P: IntoParams + Send,
    {
        let prefix = self.prefix;
        let fail_on_match = self.fail_on_match;
        let matches = Arc::clone(&self.matches);
        async move {
            if sql.starts_with(prefix)
                && matches.fetch_add(1, Ordering::Relaxed) + 1 == fail_on_match
            {
                return Err(EngineError::Runtime(
                    "injected savepoint failure".to_owned(),
                ));
            }
            SessionTemporalExec::execute(&self.inner, sql, params).await
        }
    }

    fn execute_batch(&self, sql: &str) -> impl Future<Output = Result<(), EngineError>> + Send {
        SessionTemporalExec::execute_batch(&self.inner, sql)
    }
}

impl crate::handle::SessionTemporalWriteTxn for FailingWriteTxn<'_> {
    fn commit(self) -> impl Future<Output = Result<(), EngineError>> + Send {
        crate::handle::SessionTemporalWriteTxn::commit(self.inner)
    }

    fn rollback(self) -> impl Future<Output = Result<(), EngineError>> + Send {
        crate::handle::SessionTemporalWriteTxn::rollback(self.inner)
    }
}

impl crate::handle::SessionTemporalRegisteredDb for FailingRegisteredDb<'_> {
    type WriteTxn<'b>
        = FailingWriteTxn<'b>
    where
        Self: 'b;

    fn read_snapshot(
        &self,
    ) -> impl Future<
        Output = Result<
            tracedecay_runtime_core::db::DatabaseEngineReadSnapshot,
            tracedecay_domain::errors::TraceDecayError,
        >,
    > + Send {
        crate::handle::SessionTemporalRegisteredDb::read_snapshot(self.inner)
    }

    fn health_read_snapshot(
        &self,
    ) -> impl Future<
        Output = Result<tracedecay_runtime_core::db::DatabaseEngineReadSnapshot, EngineError>,
    > + Send {
        crate::handle::SessionTemporalRegisteredDb::health_read_snapshot(self.inner)
    }

    fn begin_write_transaction(
        &self,
    ) -> impl Future<Output = Result<Self::WriteTxn<'_>, tracedecay_domain::errors::TraceDecayError>>
    + Send {
        let prefix = self.prefix;
        let fail_on_match = self.fail_on_match;
        let matches = Arc::clone(&self.matches);
        async move {
            crate::handle::SessionTemporalRegisteredDb::begin_write_transaction(self.inner)
                .await
                .map(|inner| FailingWriteTxn {
                    inner,
                    prefix,
                    fail_on_match,
                    matches,
                })
        }
    }

    fn db_path(&self) -> &std::path::Path {
        crate::handle::SessionTemporalRegisteredDb::db_path(self.inner)
    }

    fn session_relation_store(
        &self,
    ) -> Result<
        (
            crate::relations::SessionRelationScope,
            crate::relations::SessionRelationGraphStore,
        ),
        tracedecay_domain::errors::TraceDecayError,
    > {
        crate::handle::SessionTemporalRegisteredDb::session_relation_store(self.inner)
    }
}
