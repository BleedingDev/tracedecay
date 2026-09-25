type SemanticRuntimePrepareFutureV1 = Pin<
    Box<
        dyn Future<
                Output = Result<PreparedSemanticRuntimeCommitV1, SemanticRuntimeScheduleFailureV1>,
            > + Send
            + 'static,
    >,
>;
type SemanticRuntimeCommitFutureV1 = Pin<
    Box<
        dyn Future<Output = Result<SemanticGenerationPointerV1, SemanticRuntimeScheduleFailureV1>>
            + Send
            + 'static,
    >,
>;
type SemanticRuntimePublishedFutureV1 = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;
type SemanticRuntimeInstallV1 = Box<
    dyn FnOnce(&SemanticGenerationPointerV1) -> Result<(), SemanticRuntimeScheduleFailureV1>
        + Send
        + 'static,
>;
type SemanticRuntimeInstallFailureV1 = Box<
    dyn FnOnce(
            &SemanticGenerationPointerV1,
            &SemanticRuntimeScheduleFailureV1,
        ) -> Result<(), SemanticRuntimeScheduleFailureV1>
        + Send
        + 'static,
>;
type SemanticRuntimePublishedV1 = Box<
    dyn FnOnce(SemanticGenerationPointerV1, u64) -> SemanticRuntimePublishedFutureV1
        + Send
        + 'static,
>;

pub struct SemanticRuntimeScheduleCancellationV1 {
    cancelled: AtomicBool,
    completed_units: AtomicU64,
    total_units: u64,
    linked: Option<Arc<dyn crate::semantic_evaluation::SemanticEvaluationCancellationV1>>,
}

impl std::fmt::Debug for SemanticRuntimeScheduleCancellationV1 {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SemanticRuntimeScheduleCancellationV1")
            .field("cancelled", &self.cancelled.load(Ordering::Acquire))
            .field(
                "completed_units",
                &self.completed_units.load(Ordering::Acquire),
            )
            .field("total_units", &self.total_units)
            .field("linked", &self.linked.is_some())
            .finish()
    }
}

impl SemanticRuntimeScheduleCancellationV1 {
    pub fn new(total_units: u64) -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            completed_units: AtomicU64::new(0),
            total_units,
            linked: None,
        }
    }

    pub fn new_linked(
        total_units: u64,
        linked: Arc<dyn crate::semantic_evaluation::SemanticEvaluationCancellationV1>,
    ) -> Self {
        Self {
            cancelled: AtomicBool::new(false),
            completed_units: AtomicU64::new(0),
            total_units,
            linked: Some(linked),
        }
    }

    pub fn cancelled(&self) -> bool {
        self.interruption().is_some()
    }

    pub fn failure(&self) -> Option<SemanticRuntimeScheduleFailureV1> {
        self.interruption().map(|interruption| match interruption {
            SemanticExecutionInterruptionV1::Cancelled => {
                SemanticRuntimeScheduleFailureV1::Cancelled
            }
            SemanticExecutionInterruptionV1::DeadlineExceeded => {
                SemanticRuntimeScheduleFailureV1::DeadlineExceeded
            }
        })
    }

    pub fn interruption(&self) -> Option<SemanticExecutionInterruptionV1> {
        if self.cancelled.load(Ordering::Acquire) {
            return Some(SemanticExecutionInterruptionV1::Cancelled);
        }
        self.linked
            .as_ref()
            .and_then(|linked| linked.interruption())
    }

    pub fn set_completed_units(&self, completed_units: u64) -> u64 {
        let completed_units = completed_units.min(self.total_units);
        self.completed_units
            .fetch_max(completed_units, Ordering::AcqRel);
        let completed_units = self.completed_units.load(Ordering::Acquire);
        hotpath::gauge!("semantic_generation_completed_units").set(completed_units);
        completed_units
    }

    pub(crate) fn completed_units(&self) -> u64 {
        self.completed_units.load(Ordering::Acquire)
    }

    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }
}

impl SemanticExecutionAuthority for SemanticRuntimeScheduleCancellationV1 {
    fn interruption(&self) -> Option<SemanticExecutionInterruptionV1> {
        Self::interruption(self)
    }
}

pub struct PreparedSemanticRuntimeCommitV1 {
    commit: Box<dyn FnOnce() -> SemanticRuntimeCommitFutureV1 + Send + 'static>,
    install: Option<SemanticRuntimeInstallV1>,
    install_failure: Option<SemanticRuntimeInstallFailureV1>,
    published: Option<SemanticRuntimePublishedV1>,
}

impl PreparedSemanticRuntimeCommitV1 {
    pub fn new<Commit, CommitFuture>(commit: Commit) -> Self
    where
        Commit: FnOnce() -> CommitFuture + Send + 'static,
        CommitFuture: Future<Output = Result<SemanticGenerationPointerV1, SemanticRuntimeScheduleFailureV1>>
            + Send
            + 'static,
    {
        Self {
            commit: Box::new(move || Box::pin(commit())),
            install: None,
            install_failure: None,
            published: None,
        }
    }

    pub(crate) async fn commit(
        self,
    ) -> Result<
        (
            SemanticGenerationPointerV1,
            Option<SemanticRuntimeInstallV1>,
            Option<SemanticRuntimeInstallFailureV1>,
            Option<SemanticRuntimePublishedV1>,
        ),
        SemanticRuntimeScheduleFailureV1,
    > {
        let Self {
            commit,
            install,
            install_failure,
            published,
        } = self;
        commit()
            .await
            .map(|pointer| (pointer, install, install_failure, published))
    }

    pub fn on_success<Install>(mut self, install: Install) -> Self
    where
        Install: FnOnce(&SemanticGenerationPointerV1) -> Result<(), SemanticRuntimeScheduleFailureV1>
            + Send
            + 'static,
    {
        self.install = Some(Box::new(install));
        self
    }

    /// Compensate a durable publication when the warmed query runtime cannot
    /// be installed. The callback must compare-and-swap against `pointer` so a
    /// newer publication is never overwritten.
    pub fn on_install_failure<Compensate>(mut self, compensate: Compensate) -> Self
    where
        Compensate: FnOnce(
                &SemanticGenerationPointerV1,
                &SemanticRuntimeScheduleFailureV1,
            ) -> Result<(), SemanticRuntimeScheduleFailureV1>
            + Send
            + 'static,
    {
        self.install_failure = Some(Box::new(compensate));
        self
    }

    /// Observe only after the query runtime and scheduler pointer are installed.
    pub fn on_published<Published, PublishedFuture>(self, published: Published) -> Self
    where
        Published: FnOnce(SemanticGenerationPointerV1) -> PublishedFuture + Send + 'static,
        PublishedFuture: Future<Output = ()> + Send + 'static,
    {
        self.on_published_with_token(move |pointer, _publication_token| published(pointer))
    }

    /// Observe a publication with the scheduler's opaque transition token.
    /// The token remains bound to this worker even if a later worker reuses
    /// every field of the generation pointer.
    pub fn on_published_with_token<Published, PublishedFuture>(
        mut self,
        published: Published,
    ) -> Self
    where
        Published: FnOnce(SemanticGenerationPointerV1, u64) -> PublishedFuture + Send + 'static,
        PublishedFuture: Future<Output = ()> + Send + 'static,
    {
        self.published = Some(Box::new(move |pointer, publication_token| {
            Box::pin(published(pointer, publication_token))
        }));
        self
    }
}

pub struct SemanticRuntimeWorkV1 {
    target_generation: CodeGenerationId,
    target_projection_key: Option<ProjectionKeyV1>,
    total_units: u64,
    prepare: Box<
        dyn FnOnce(Arc<SemanticRuntimeScheduleCancellationV1>) -> SemanticRuntimePrepareFutureV1
            + Send
            + 'static,
    >,
}

impl SemanticRuntimeWorkV1 {
    pub fn new<Prepare, PrepareFuture>(
        target_generation: CodeGenerationId,
        total_units: u64,
        prepare: Prepare,
    ) -> Self
    where
        Prepare:
            FnOnce(Arc<SemanticRuntimeScheduleCancellationV1>) -> PrepareFuture + Send + 'static,
        PrepareFuture: Future<
                Output = Result<PreparedSemanticRuntimeCommitV1, SemanticRuntimeScheduleFailureV1>,
            > + Send
            + 'static,
    {
        Self {
            target_generation,
            target_projection_key: None,
            total_units: total_units.max(1),
            prepare: Box::new(move |cancellation| Box::pin(prepare(cancellation))),
        }
    }

    pub fn new_with_projection<Prepare, PrepareFuture>(
        target_generation: CodeGenerationId,
        target_projection_key: ProjectionKeyV1,
        total_units: u64,
        prepare: Prepare,
    ) -> Self
    where
        Prepare:
            FnOnce(Arc<SemanticRuntimeScheduleCancellationV1>) -> PrepareFuture + Send + 'static,
        PrepareFuture: Future<
                Output = Result<PreparedSemanticRuntimeCommitV1, SemanticRuntimeScheduleFailureV1>,
            > + Send
            + 'static,
    {
        let mut work = Self::new(target_generation, total_units, prepare);
        work.target_projection_key = Some(target_projection_key);
        work
    }

    pub fn total_units(&self) -> u64 {
        self.total_units
    }
}

struct SemanticRuntimeSchedulingStateV1 {
    sequence: u64,
    status: SemanticRuntimeScheduleStatusV1,
    current: Option<SemanticGenerationPointerV1>,
    /// Unique token for the publication currently represented by `current`.
    /// The pointer alone is insufficient because a replacement may publish
    /// the same vector generation and projection again.
    current_publication_token: Option<u64>,
    /// A restore rollback may only compensate the `restore_current` transition
    /// that created it. Any accepted schedule invalidates this token, even if
    /// the newer work has not published a pointer yet.
    restore_rollback_token: Option<u64>,
    cancellation: Option<Arc<SemanticRuntimeScheduleCancellationV1>>,
    committing: bool,
    accepting_work: bool,
}

impl Default for SemanticRuntimeSchedulingStateV1 {
    fn default() -> Self {
        Self {
            sequence: 0,
            status: SemanticRuntimeScheduleStatusV1::Unavailable,
            current: None,
            current_publication_token: None,
            restore_rollback_token: None,
            cancellation: None,
            committing: false,
            accepting_work: true,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SemanticRuntimeShutdownReceiptV1 {
    pub joined_workers: usize,
    pub aborted_workers: usize,
    pub remaining_workers: usize,
}

impl SemanticRuntimeShutdownReceiptV1 {
    pub fn is_clean(&self) -> bool {
        self.remaining_workers == 0
    }
}

#[derive(Clone)]
pub struct SemanticRuntimeSchedulingHandleV1 {
    state: Arc<Mutex<SemanticRuntimeSchedulingStateV1>>,
    workers: Arc<Mutex<BTreeMap<u64, JoinHandle<()>>>>,
    commit_completions: tokio::sync::watch::Sender<u64>,
}

struct SemanticGenerationActiveGaugeV1;

impl SemanticGenerationActiveGaugeV1 {
    fn enter() -> Self {
        hotpath::gauge!("semantic_generation_active_workers").inc(1.0);
        Self
    }
}

impl Drop for SemanticGenerationActiveGaugeV1 {
    fn drop(&mut self) {
        hotpath::gauge!("semantic_generation_active_workers").dec(1.0);
    }
}

impl Default for SemanticRuntimeSchedulingHandleV1 {
    fn default() -> Self {
        let (commit_completions, _receiver) = tokio::sync::watch::channel(0);
        Self {
            state: Arc::new(Mutex::new(SemanticRuntimeSchedulingStateV1::default())),
            workers: Arc::new(Mutex::new(BTreeMap::new())),
            commit_completions,
        }
    }
}

impl SemanticRuntimeSchedulingHandleV1 {
    pub fn new() -> Self {
        Self::default()
    }

    /// Observe completion of a serialized publication commit.
    ///
    /// A caller refused while a commit is in progress subscribes once and
    /// retries its still-current source only after this epoch advances. The
    /// signal is not emitted for resource refusal, so it cannot create a
    /// failure retry loop.
    pub fn commit_completions(&self) -> tokio::sync::watch::Receiver<u64> {
        self.commit_completions.subscribe()
    }

    fn notify_commit_completed(&self) {
        self.commit_completions
            .send_modify(|epoch| *epoch = epoch.wrapping_add(1));
    }

    /// Start one bounded preparation task without waiting for artifact I/O,
    /// model loading, projection, or publication.
    ///
    /// A task already inside its serialized atomic commit is not displaced;
    /// callers receive `false` and may schedule the newer generation again.
    pub fn schedule(&self, work: SemanticRuntimeWorkV1) -> bool {
        let mut workers = self.workers.lock().unwrap_or_else(PoisonError::into_inner);
        workers.retain(|_, worker| !worker.is_finished());
        let (sequence, cancellation) = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            if !state.accepting_work || state.committing {
                return false;
            }
            if let Some(cancellation) = state.cancellation.take() {
                cancellation.cancel();
            }
            state.sequence = state.sequence.wrapping_add(1);
            let sequence = state.sequence;
            state.restore_rollback_token = None;
            let cancellation =
                Arc::new(SemanticRuntimeScheduleCancellationV1::new(work.total_units));
            hotpath::gauge!("semantic_generation_total_units").set(work.total_units);
            hotpath::gauge!("semantic_generation_completed_units").set(0_u64);
            state.status = SemanticRuntimeScheduleStatusV1::Indexing {
                target_generation: work.target_generation.clone(),
                target_projection_key: work.target_projection_key.clone(),
                completed_units: 0,
                total_units: work.total_units,
                prior_generation: state
                    .current
                    .as_ref()
                    .map(|pointer| pointer.generation.clone()),
            };
            state.cancellation = Some(Arc::clone(&cancellation));
            (sequence, cancellation)
        };

        let handle = self.clone();
        let watcher = self.clone();
        let worker = tokio::spawn(async move {
            let worker = tokio::spawn(hotpath::future!(
                async move {
                    let _active = SemanticGenerationActiveGaugeV1::enter();
                    let prepared = hotpath::future!(
                        (work.prepare)(Arc::clone(&cancellation)),
                        label = "semantic.runtime.generation.prepare"
                    )
                    .await;
                    let prepared = match prepared {
                        Ok(prepared) if !cancellation.cancelled() => prepared,
                        Ok(_) => {
                            handle.finish_failure(
                                sequence,
                                SemanticRuntimeScheduleFailureV1::Cancelled,
                            );
                            return;
                        }
                        Err(reason) => {
                            handle.finish_failure(sequence, reason);
                            return;
                        }
                    };

                    {
                        let mut state = handle.state.lock().unwrap_or_else(PoisonError::into_inner);
                        if state.sequence != sequence
                            || cancellation.cancelled()
                            || state.committing
                        {
                            return;
                        }
                        state.committing = true;
                    }

                    let committed = hotpath::future!(
                        prepared.commit(),
                        label = "semantic.runtime.generation.publish"
                    )
                    .await;
                    let published = {
                        let mut state = handle.state.lock().unwrap_or_else(PoisonError::into_inner);
                        if state.sequence != sequence
                            || cancellation.cancelled()
                            || !state.accepting_work
                        {
                            state.committing = false;
                            drop(state);
                            handle.notify_commit_completed();
                            return;
                        }
                        let published = match committed {
                            Ok((pointer, install, install_failure, published)) => {
                                if let Some(install) = install
                                    && let Err(install_reason) = hotpath::measure_block!(
                                        "semantic.runtime.generation.install",
                                        install(&pointer)
                                    )
                                {
                                    let reason = match install_failure {
                                        Some(compensate) => match hotpath::measure_block!(
                                            "semantic.runtime.generation.compensate_install",
                                            compensate(&pointer, &install_reason)
                                        ) {
                                            Ok(()) => install_reason,
                                            Err(compensation_reason) => compensation_reason,
                                        },
                                        None => install_reason,
                                    };
                                    state.committing = false;
                                    state.cancellation = None;
                                    state.status = SemanticRuntimeScheduleStatusV1::Failed {
                                        reason,
                                        prior_generation: state
                                            .current
                                            .as_ref()
                                            .map(|pointer| pointer.generation.clone()),
                                    };
                                    drop(state);
                                    handle.notify_commit_completed();
                                    return;
                                }
                                state.current = Some(pointer.clone());
                                state.current_publication_token = Some(sequence);
                                state.status = SemanticRuntimeScheduleStatusV1::Current {
                                    generation: pointer.generation.clone(),
                                };
                                published.map(|published| (published, pointer))
                            }
                            Err(reason) => {
                                state.status = SemanticRuntimeScheduleStatusV1::Failed {
                                    reason,
                                    prior_generation: state
                                        .current
                                        .as_ref()
                                        .map(|pointer| pointer.generation.clone()),
                                };
                                None
                            }
                        };
                        state.committing = false;
                        state.cancellation = None;
                        published
                    };
                    handle.notify_commit_completed();
                    if let Some((published, pointer)) = published {
                        hotpath::future!(
                            published(pointer, sequence),
                            label = "semantic.runtime.generation.observe_published"
                        )
                        .await;
                    }
                },
                label = "semantic.runtime.generation"
            ));
            let mut abort_on_drop = AbortWorkerOnDrop::new(worker.abort_handle());
            let outcome = worker.await;
            abort_on_drop.disarm();
            if outcome.is_err() {
                watcher.finish_worker_terminated(sequence);
            }
        });
        workers.insert(sequence, worker);
        true
    }

    /// Permanently fence new projection work and signal the active worker.
    pub fn begin_shutdown(&self) -> bool {
        let _workers = self.workers.lock().unwrap_or_else(PoisonError::into_inner);
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if !state.accepting_work {
            return false;
        }
        state.accepting_work = false;
        if let Some(cancellation) = state.cancellation.as_ref() {
            cancellation.cancel();
        }
        true
    }

    /// Join every projection worker against the caller's one global deadline.
    ///
    /// Workers remaining at the deadline are aborted and then awaited before
    /// this method returns, so a clean receipt proves no worker escaped.
    pub async fn cancel_and_join_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> SemanticRuntimeShutdownReceiptV1 {
        self.begin_shutdown();
        let mut workers = {
            let mut registry = self.workers.lock().unwrap_or_else(PoisonError::into_inner);
            std::mem::take(&mut *registry)
                .into_iter()
                .collect::<Vec<_>>()
        };
        let mut joined_workers = 0;
        let mut aborted_workers = 0;

        let now = tokio::time::Instant::now();
        let abort_at = deadline
            .checked_sub(Duration::from_millis(50))
            .unwrap_or(now)
            .max(now);
        while !workers.is_empty() && tokio::time::Instant::now() < abort_at {
            match tokio::time::timeout_at(abort_at, join_next_worker(&mut workers)).await {
                Ok(true) => joined_workers += 1,
                Ok(false) | Err(_) => break,
            }
        }

        if !workers.is_empty() {
            aborted_workers = workers.len();
            for (_, worker) in &workers {
                worker.abort();
            }
            self.normalize_shutdown_terminal(SemanticRuntimeScheduleFailureV1::Cancelled);
        }
        while !workers.is_empty() && tokio::time::Instant::now() < deadline {
            match tokio::time::timeout_at(deadline, join_next_worker(&mut workers)).await {
                Ok(true) => {}
                Ok(false) | Err(_) => break,
            }
        }
        let mut registry = self.workers.lock().unwrap_or_else(PoisonError::into_inner);
        for (sequence, worker) in workers {
            registry.insert(sequence, worker);
        }
        let remaining_workers = registry.len();
        drop(registry);
        SemanticRuntimeShutdownReceiptV1 {
            joined_workers,
            aborted_workers,
            remaining_workers,
        }
    }

    pub fn status(&self) -> SemanticRuntimeScheduleStatusV1 {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let mut status = state.status.clone();
        if let (
            SemanticRuntimeScheduleStatusV1::Indexing {
                completed_units, ..
            },
            Some(cancellation),
        ) = (&mut status, state.cancellation.as_ref())
        {
            *completed_units = cancellation.completed_units();
        }
        status
    }

    pub fn current(&self) -> Option<SemanticGenerationPointerV1> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .current
            .clone()
    }

    pub fn restore_current(&self, pointer: SemanticGenerationPointerV1) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(cancellation) = state.cancellation.take() {
            cancellation.cancel();
        }
        state.sequence = state.sequence.wrapping_add(1);
        state.committing = false;
        state.current = Some(pointer.clone());
        state.current_publication_token = Some(state.sequence);
        state.restore_rollback_token = Some(state.sequence);
        state.status = SemanticRuntimeScheduleStatusV1::Current {
            generation: pointer.generation,
        };
    }

    /// Restore a pointer/status snapshot only while the pointer installed by
    /// the transaction is still current. A lifecycle persistence failure must
    /// not undo a newer scheduled generation that won the scheduler race.
    pub fn restore_snapshot_if_current(
        &self,
        installed: &SemanticGenerationPointerV1,
        current: Option<SemanticGenerationPointerV1>,
        status: SemanticRuntimeScheduleStatusV1,
    ) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.committing
            || state.current.as_ref() != Some(installed)
            || state.restore_rollback_token.is_none()
            || state.status
                != (SemanticRuntimeScheduleStatusV1::Current {
                    generation: installed.generation.clone(),
                })
        {
            return false;
        }
        if let Some(cancellation) = state.cancellation.take() {
            cancellation.cancel();
        }
        state.sequence = state.sequence.wrapping_add(1);
        state.current = current;
        state.current_publication_token = state.current.as_ref().map(|_| state.sequence);
        state.restore_rollback_token = None;
        state.status = status;
        true
    }

    /// Return the opaque publication token for an exact current pointer.
    /// Distinct lifecycle artifacts may intentionally reuse the same pointer.
    pub fn current_publication_token_for(
        &self,
        expected: &SemanticGenerationPointerV1,
    ) -> Option<u64> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        (state.current.as_ref() == Some(expected))
            .then_some(state.current_publication_token)
            .flatten()
    }

    /// Run one cache/runtime publication step while the exact scheduler
    /// publication is reserved. Holding the state lock across `commit`
    /// prevents a worker from replacing the pointer between validation and
    /// the caller's mutation.
    pub fn with_current_publication<R>(
        &self,
        expected: &SemanticGenerationPointerV1,
        publication_token: u64,
        commit: impl FnOnce() -> R,
    ) -> Option<R> {
        let _state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if _state.current.as_ref() != Some(expected)
            || _state.current_publication_token != Some(publication_token)
            || _state.status
                != (SemanticRuntimeScheduleStatusV1::Current {
                    generation: expected.generation.clone(),
                })
        {
            return None;
        }
        Some(commit())
    }

    /// Clear only the publication represented by `expected` and `token`.
    /// Pointer equality by itself would let a late callback for artifact A
    /// erase artifact B when both artifacts share a vector generation.
    pub fn clear_current_if_with_publication(
        &self,
        expected: &SemanticGenerationPointerV1,
        token: u64,
    ) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.committing
            || state.current.as_ref() != Some(expected)
            || state.current_publication_token != Some(token)
            || state.status
                != (SemanticRuntimeScheduleStatusV1::Current {
                    generation: expected.generation.clone(),
                })
        {
            return false;
        }
        if let Some(cancellation) = state.cancellation.take() {
            cancellation.cancel();
        }
        state.sequence = state.sequence.wrapping_add(1);
        state.current = None;
        state.current_publication_token = None;
        state.restore_rollback_token = None;
        state.status = SemanticRuntimeScheduleStatusV1::Unavailable;
        true
    }

    pub fn clear_current_if(&self, expected: &SemanticGenerationPointerV1) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.committing || state.current.as_ref() != Some(expected) {
            return false;
        }
        if let Some(cancellation) = state.cancellation.take() {
            cancellation.cancel();
        }
        state.sequence = state.sequence.wrapping_add(1);
        state.current = None;
        state.current_publication_token = None;
        state.restore_rollback_token = None;
        state.status = SemanticRuntimeScheduleStatusV1::Unavailable;
        true
    }

    pub fn cancel(&self) -> bool {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.committing {
            return false;
        }
        let Some(cancellation) = state.cancellation.take() else {
            return false;
        };
        cancellation.cancel();
        state.sequence = state.sequence.wrapping_add(1);
        state.status = SemanticRuntimeScheduleStatusV1::Failed {
            reason: SemanticRuntimeScheduleFailureV1::Cancelled,
            prior_generation: state
                .current
                .as_ref()
                .map(|pointer| pointer.generation.clone()),
        };
        true
    }

    fn finish_failure(&self, sequence: u64, reason: SemanticRuntimeScheduleFailureV1) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.sequence == sequence && !state.committing {
            state.cancellation = None;
            state.status = SemanticRuntimeScheduleStatusV1::Failed {
                reason,
                prior_generation: state
                    .current
                    .as_ref()
                    .map(|pointer| pointer.generation.clone()),
            };
        }
    }

    fn finish_worker_terminated(&self, sequence: u64) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        if state.sequence == sequence {
            let was_committing = state.committing;
            state.committing = false;
            state.cancellation = None;
            state.status = SemanticRuntimeScheduleStatusV1::Failed {
                reason: SemanticRuntimeScheduleFailureV1::Runtime,
                prior_generation: state
                    .current
                    .as_ref()
                    .map(|pointer| pointer.generation.clone()),
            };
            drop(state);
            if was_committing {
                self.notify_commit_completed();
            }
        }
    }

    fn normalize_shutdown_terminal(&self, reason: SemanticRuntimeScheduleFailureV1) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let was_committing = state.committing;
        state.committing = false;
        state.cancellation = None;
        state.status = SemanticRuntimeScheduleStatusV1::Failed {
            reason,
            prior_generation: state
                .current
                .as_ref()
                .map(|pointer| pointer.generation.clone()),
        };
        drop(state);
        if was_committing {
            self.notify_commit_completed();
        }
    }
}

impl crate::DaemonSemanticRuntimeHandleV1 {
    pub fn schedule_commit_completions(&self) -> tokio::sync::watch::Receiver<u64> {
        self.scheduling.commit_completions()
    }

    /// Capture the scheduler's opaque publication token for an exact pointer.
    /// Callers that may outlive the callback must retain this token alongside
    /// the lifecycle artifact identity before another schedule can publish.
    pub fn current_publication_token_for(
        &self,
        expected: &SemanticGenerationPointerV1,
    ) -> Option<u64> {
        self.scheduling.current_publication_token_for(expected)
    }

    /// Remove one exact publication from both scheduler and query runtime.
    /// A late callback cannot clear a replacement that reused the same pointer.
    pub fn unbind_query_runtime_if_current_with_publication(
        &self,
        expected: &SemanticGenerationPointerV1,
        publication_token: u64,
    ) -> bool {
        let _transition = self
            .transitions
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if !self
            .scheduling
            .clear_current_if_with_publication(expected, publication_token)
        {
            return false;
        }
        *self.runtime.write().unwrap_or_else(PoisonError::into_inner) = None;
        true
    }

    /// Run one cache/runtime publication step while the exact scheduler
    /// publication is reserved. A newer schedule cannot replace the pointer
    /// between the identity check and the caller's mutation.
    pub fn with_current_publication<R>(
        &self,
        expected: &SemanticGenerationPointerV1,
        publication_token: u64,
        commit: impl FnOnce() -> R,
    ) -> Option<R> {
        self.scheduling
            .with_current_publication(expected, publication_token, commit)
    }
}

struct AbortWorkerOnDrop {
    handle: Option<tokio::task::AbortHandle>,
}

impl AbortWorkerOnDrop {
    fn new(handle: tokio::task::AbortHandle) -> Self {
        Self {
            handle: Some(handle),
        }
    }

    fn disarm(&mut self) {
        self.handle = None;
    }
}

impl Drop for AbortWorkerOnDrop {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}

async fn join_next_worker(workers: &mut Vec<(u64, JoinHandle<()>)>) -> bool {
    std::future::poll_fn(|context| {
        for index in (0..workers.len()).rev() {
            if Pin::new(&mut workers[index].1).poll(context).is_ready() {
                let _ = workers.swap_remove(index);
                return std::task::Poll::Ready(true);
            }
        }
        if workers.is_empty() {
            std::task::Poll::Ready(false)
        } else {
            std::task::Poll::Pending
        }
    })
    .await
}

#[cfg(test)]
mod schedule_failure_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use tracedecay_domain::{ManifestDigest, VectorGenerationIdV1};

    fn same_pointer() -> SemanticGenerationPointerV1 {
        SemanticGenerationPointerV1 {
            generation: VectorGenerationIdV1::new(
                ManifestDigest::new(format!("sha256:{}", "a".repeat(64)))
                    .expect("vector generation digest"),
            ),
            source_generation: CodeGenerationId::new("schedule-race-source")
                .expect("source generation"),
            projection_key: crate::session_pool::test_support::authority()
                .projection()
                .projection_key()
                .clone(),
        }
    }

    #[test]
    fn completed_units_ignore_out_of_order_regressions() {
        let progress = SemanticRuntimeScheduleCancellationV1::new(8);

        assert_eq!(progress.set_completed_units(5), 5);
        assert_eq!(progress.set_completed_units(3), 5);
        assert_eq!(progress.completed_units(), 5);
    }

    #[tokio::test]
    async fn restore_snapshot_cannot_undo_same_pointer_newer_publication() {
        let handle =
            crate::DaemonSemanticRuntimeHandleV1::new(1, 8, 1 << 20).expect("semantic handle");
        let pointer = same_pointer();
        handle.scheduling.restore_current(pointer.clone());
        let (published_tx, published_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let scheduled_pointer = pointer.clone();
        assert!(handle.schedule(SemanticRuntimeWorkV1::new_with_projection(
            pointer.source_generation.clone(),
            pointer.projection_key.clone(),
            1,
            move |_cancellation| async move {
                Ok(PreparedSemanticRuntimeCommitV1::new(
                    move || async move { Ok(scheduled_pointer) },
                )
                .on_published(move |_pointer| async move {
                    let _ = published_tx.send(());
                    let _ = release_rx.await;
                }))
            },
        )));
        published_rx.await.expect("replacement was published");

        assert!(!handle.scheduling.restore_snapshot_if_current(
            &pointer,
            None,
            SemanticRuntimeScheduleStatusV1::Unavailable,
        ));
        assert_eq!(handle.current(), Some(pointer));

        release_tx.send(()).expect("release replacement callback");
    }

    #[tokio::test]
    async fn failed_runtime_install_compensates_before_preserving_previous_generation() {
        let handle =
            crate::DaemonSemanticRuntimeHandleV1::new(1, 8, 1 << 20).expect("semantic handle");
        let previous = same_pointer();
        handle.scheduling.restore_current(previous.clone());
        let next = SemanticGenerationPointerV1 {
            generation: VectorGenerationIdV1::new(
                ManifestDigest::new(format!("sha256:{}", "b".repeat(64)))
                    .expect("vector generation digest"),
            ),
            source_generation: CodeGenerationId::new("schedule-install-failure-source")
                .expect("source generation"),
            projection_key: previous.projection_key.clone(),
        };
        let compensated = Arc::new(AtomicBool::new(false));
        let observed = Arc::clone(&compensated);
        let scheduled = next.clone();

        assert!(handle.schedule(SemanticRuntimeWorkV1::new_with_projection(
            next.source_generation.clone(),
            next.projection_key.clone(),
            1,
            move |_cancellation| async move {
                Ok(
                    PreparedSemanticRuntimeCommitV1::new(move || async move { Ok(scheduled) })
                        .on_success(|_| Err(SemanticRuntimeScheduleFailureV1::Runtime))
                        .on_install_failure(move |pointer, reason| {
                            assert_eq!(pointer, &next);
                            assert_eq!(reason, &SemanticRuntimeScheduleFailureV1::Runtime);
                            observed.store(true, Ordering::Release);
                            Ok(())
                        }),
                )
            },
        )));

        tokio::time::timeout(Duration::from_secs(1), async {
            while !matches!(
                handle.status(),
                SemanticRuntimeScheduleStatusV1::Failed { .. }
            ) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("failed installation reaches a terminal state");

        assert!(compensated.load(Ordering::Acquire));
        assert_eq!(handle.current(), Some(previous));
    }

    #[tokio::test]
    async fn refused_during_commit_is_signalled_only_after_commit_finishes() {
        let handle =
            crate::DaemonSemanticRuntimeHandleV1::new(1, 8, 1 << 20).expect("semantic handle");
        let mut completions = handle.schedule_commit_completions();
        let (commit_started_tx, commit_started_rx) = tokio::sync::oneshot::channel();
        let (release_commit_tx, release_commit_rx) = tokio::sync::oneshot::channel();
        let first = same_pointer();
        let installed = first.clone();

        assert!(handle.schedule(SemanticRuntimeWorkV1::new_with_projection(
            first.source_generation.clone(),
            first.projection_key.clone(),
            1,
            move |_cancellation| async move {
                Ok(PreparedSemanticRuntimeCommitV1::new(move || async move {
                    let _ = commit_started_tx.send(());
                    let _ = release_commit_rx.await;
                    Ok(installed)
                }))
            },
        )));
        commit_started_rx.await.expect("first commit started");

        let refused = same_pointer();
        assert!(!handle.schedule(SemanticRuntimeWorkV1::new_with_projection(
            refused.source_generation.clone(),
            refused.projection_key.clone(),
            1,
            move |_cancellation| async move {
                Ok(PreparedSemanticRuntimeCommitV1::new(move || async move {
                    Ok(refused)
                }))
            },
        )));
        assert!(matches!(completions.has_changed(), Ok(false)));

        release_commit_tx.send(()).expect("release first commit");
        tokio::time::timeout(Duration::from_secs(1), completions.changed())
            .await
            .expect("commit completion was signalled")
            .expect("scheduler still owns completion sender");
        assert!(matches!(
            handle.status(),
            SemanticRuntimeScheduleStatusV1::Current { .. }
        ));
    }
}
