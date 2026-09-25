//! Project SQLite durability for the immutable vector-generation authority.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use tracedecay_store::runtime::{
    BatchCommitDecisionV1, ContentDigest, DurableVectorAuthorityStoreV1,
    PreparedVectorGenerationV1, PublishedVectorGenerationV1, VectorAuthorityError,
    VectorAuthorityRevisionV1, VectorAuthorityStoreErrorV1, VectorGenerationAuthority,
    VectorGenerationBuildIdV1, VectorGenerationIdV1, VectorGenerationPlanV1,
    VectorGenerationPublicationV1, VectorProjectionCheckpointV1,
};

use crate::exact_sql::{
    ExactSqlHandle, ExactSqlRow, ExactSqlStatement, ExactSqlTransaction, ExactSqlValue,
};

pub const VECTOR_AUTHORITY_SCHEMA_V1: &str = include_str!("vector_authority_schema.sql");
pub const VECTOR_AUTHORITY_OBJECTS_V1: &[&str] = &[
    "vector_authority_heads_v1",
    "vector_authority_stages_v1",
    "vector_authority_batches_v1",
    "vector_authority_generations_v1",
    "vector_authority_float_blobs_v1",
    "vector_authority_generation_blobs_v1",
];

fn validate_authority_namespace(
    authority_namespace: &str,
) -> Result<String, VectorAuthorityStoreErrorV1> {
    if authority_namespace.is_empty() {
        return Err(VectorAuthorityStoreErrorV1::Unavailable(
            "vector authority requires a stable repository/worktree namespace".to_owned(),
        ));
    }
    Ok(authority_namespace.to_owned())
}

#[derive(Clone)]
pub struct VectorAuthoritySqliteStorage {
    handle: ExactSqlHandle,
    _guard: Arc<dyn Send + Sync>,
}

impl VectorAuthoritySqliteStorage {
    pub fn from_authorized_handle(
        handle: ExactSqlHandle,
    ) -> Result<Self, VectorAuthorityStoreErrorV1> {
        Self::from_authorized_handle_with_guard(handle, ())
    }

    pub fn from_authorized_handle_with_guard<Guard>(
        handle: ExactSqlHandle,
        guard: Guard,
    ) -> Result<Self, VectorAuthorityStoreErrorV1>
    where
        Guard: Send + Sync + 'static,
    {
        if !matches!(
            &handle.binding().shard_id.scope,
            tracedecay_store::StoreShardScopeV1::Project { .. }
        ) {
            return Err(VectorAuthorityStoreErrorV1::Unavailable(
                "vector authority requires a project shard".to_owned(),
            ));
        }
        Ok(Self {
            handle,
            _guard: Arc::new(guard),
        })
    }

    pub fn open(
        &self,
        authority_namespace: &str,
    ) -> Result<ProjectVectorAuthorityHandleV1, VectorAuthorityStoreErrorV1> {
        let authority_namespace = validate_authority_namespace(authority_namespace)?;
        let transaction = self.handle.begin_immediate().map_err(unavailable)?;
        let loaded = load_opened_state(&transaction, &authority_namespace);
        match loaded {
            Ok(state) => {
                transaction.rollback().map_err(unavailable)?;
                Ok(ProjectVectorAuthorityHandleV1 {
                    handle: self.handle.clone(),
                    _guard: Arc::clone(&self._guard),
                    authority_namespace,
                    state: Arc::new(Mutex::new(state)),
                })
            }
            Err(error) => {
                let _ = transaction.rollback();
                Err(error)
            }
        }
    }
}

struct OpenedVectorAuthorityV1 {
    revision: VectorAuthorityRevisionV1,
    authority: Arc<VectorGenerationAuthority>,
    staged_build_ids: BTreeSet<VectorGenerationBuildIdV1>,
}

#[derive(Clone)]
pub struct ProjectVectorAuthorityHandleV1 {
    handle: ExactSqlHandle,
    _guard: Arc<dyn Send + Sync>,
    authority_namespace: String,
    state: Arc<Mutex<OpenedVectorAuthorityV1>>,
}

impl ProjectVectorAuthorityHandleV1 {
    pub fn authority_namespace(&self) -> &str {
        &self.authority_namespace
    }

    pub fn revision(&self) -> Result<VectorAuthorityRevisionV1, VectorAuthorityStoreErrorV1> {
        DurableVectorAuthorityStoreV1::revision(self)
    }

    pub fn snapshot(&self) -> Result<Arc<VectorGenerationAuthority>, VectorAuthorityStoreErrorV1> {
        DurableVectorAuthorityStoreV1::snapshot(self)
    }

    pub fn begin_generation(
        &self,
        plan: VectorGenerationPlanV1,
    ) -> Result<VectorGenerationBuildIdV1, VectorAuthorityStoreErrorV1> {
        DurableVectorAuthorityStoreV1::begin_generation(self, plan)
    }

    pub fn commit_batch(
        &self,
        build_id: &VectorGenerationBuildIdV1,
        expected_checkpoint: Option<&VectorProjectionCheckpointV1>,
        prepared: PreparedVectorGenerationV1,
    ) -> Result<VectorProjectionCheckpointV1, VectorAuthorityStoreErrorV1> {
        DurableVectorAuthorityStoreV1::commit_batch(self, build_id, expected_checkpoint, prepared)
    }

    pub fn publish_generation_if_current(
        &self,
        build_id: &VectorGenerationBuildIdV1,
        expected_active: Option<&VectorGenerationIdV1>,
    ) -> Result<VectorGenerationPublicationV1, VectorAuthorityStoreErrorV1> {
        DurableVectorAuthorityStoreV1::publish_generation_if_current(
            self,
            build_id,
            expected_active,
        )
    }

    pub fn activate_generation_if_current(
        &self,
        generation_id: &VectorGenerationIdV1,
        expected_active: Option<&VectorGenerationIdV1>,
    ) -> Result<Option<VectorGenerationIdV1>, VectorAuthorityStoreErrorV1> {
        DurableVectorAuthorityStoreV1::activate_generation_if_current(
            self,
            generation_id,
            expected_active,
        )
    }

    pub fn rollback_generation_if_current(
        &self,
        expected_active: Option<&VectorGenerationIdV1>,
    ) -> Result<VectorGenerationIdV1, VectorAuthorityStoreErrorV1> {
        DurableVectorAuthorityStoreV1::rollback_generation_if_current(self, expected_active)
    }

    pub fn restore_active_generation_if_current(
        &self,
        expected_active: &VectorGenerationIdV1,
        replacement: Option<&VectorGenerationIdV1>,
    ) -> Result<(), VectorAuthorityStoreErrorV1> {
        DurableVectorAuthorityStoreV1::restore_active_generation_if_current(
            self,
            expected_active,
            replacement,
        )
    }

    fn lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, OpenedVectorAuthorityV1>, VectorAuthorityStoreErrorV1>
    {
        self.state.lock().map_err(|_| {
            VectorAuthorityStoreErrorV1::Unavailable(
                "vector-authority runtime lock is poisoned".to_owned(),
            )
        })
    }

    fn recover_after_committed_apply_failure<T>(
        &self,
        state: &mut OpenedVectorAuthorityV1,
        message: impl Into<String>,
    ) -> Result<T, VectorAuthorityStoreErrorV1> {
        let message = message.into();
        let transaction = self.handle.begin_immediate().map_err(unavailable)?;
        let recovered = load_opened_state(&transaction, &self.authority_namespace);
        let rolled_back = transaction.rollback().map_err(unavailable);
        match (recovered, rolled_back) {
            (Ok(recovered), Ok(_)) => {
                *state = recovered;
                Err(corrupt(message))
            }
            (Err(error), _) | (_, Err(error)) => Err(corrupt(format!(
                "{message}; committed state could not be reopened: {error}"
            ))),
        }
    }
}

impl DurableVectorAuthorityStoreV1 for ProjectVectorAuthorityHandleV1 {
    fn revision(&self) -> Result<VectorAuthorityRevisionV1, VectorAuthorityStoreErrorV1> {
        Ok(self.lock()?.revision)
    }

    fn snapshot(&self) -> Result<Arc<VectorGenerationAuthority>, VectorAuthorityStoreErrorV1> {
        Ok(Arc::clone(&self.lock()?.authority))
    }

    fn begin_generation(
        &self,
        plan: VectorGenerationPlanV1,
    ) -> Result<VectorGenerationBuildIdV1, VectorAuthorityStoreErrorV1> {
        plan.validate()?;
        let mut state = self.lock()?;
        let build_id = plan.build_id()?;
        if state.staged_build_ids.contains(&build_id) {
            return Ok(build_id);
        }
        let mut candidate = (*state.authority).clone();
        for staged in &state.staged_build_ids {
            if !candidate.cancel_generation(staged) {
                return Err(corrupt(
                    "tracked durable vector stage is absent from reducer state",
                ));
            }
        }
        let candidate_build_id = candidate.begin_generation(plan.clone())?;
        if candidate_build_id != build_id {
            return Err(corrupt(
                "vector generation plan produced inconsistent build identities",
            ));
        }
        let encoded_build = build_id.to_string();
        let checkpoint = candidate
            .staged_checkpoint(&build_id)
            .ok_or_else(|| corrupt("new vector build has no checkpoint"))?;
        let next = state.revision.next()?;
        let transaction = self.handle.begin_immediate().map_err(unavailable)?;
        require_revision(&transaction, &self.authority_namespace, state.revision)?;
        ensure_head(&transaction, &self.authority_namespace, state.revision)?;
        let deleted = execute(
            &transaction,
            "DELETE FROM vector_authority_stages_v1 WHERE authority_namespace = ?1",
            vec![text(&self.authority_namespace)],
        )?;
        if deleted != state.staged_build_ids.len() {
            return Err(corrupt(
                "durable vector stage inventory changed without advancing its head",
            ));
        }
        execute(
            &transaction,
            "INSERT INTO vector_authority_stages_v1 (
                 authority_namespace, build_id, projection_key, plan_digest,
                 canonical_plan, checkpoint
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            vec![
                text(&self.authority_namespace),
                text(&encoded_build),
                text(&serde_json::to_string(&plan.target_projection_key).map_err(serialization)?),
                text(plan.identity_digest()?.as_str()),
                blob_json(&plan)?,
                blob_json(checkpoint)?,
            ],
        )?;
        update_head_pointers(
            &transaction,
            &self.authority_namespace,
            state.revision,
            next,
            candidate.active_generation(),
            candidate.rollback_generation(),
        )?;
        transaction.commit().map_err(unavailable)?;
        state.revision = next;
        state.staged_build_ids.clear();
        state.staged_build_ids.insert(build_id.clone());
        state.authority = Arc::new(candidate);
        Ok(build_id)
    }

    fn commit_batch(
        &self,
        build_id: &VectorGenerationBuildIdV1,
        expected_checkpoint: Option<&VectorProjectionCheckpointV1>,
        prepared: PreparedVectorGenerationV1,
    ) -> Result<VectorProjectionCheckpointV1, VectorAuthorityStoreErrorV1> {
        let mut state = self.lock()?;
        if !state.staged_build_ids.contains(build_id) {
            return Err(VectorAuthorityError::UnknownBuild.into());
        }
        let decision = state
            .authority
            .validate_batch(build_id, expected_checkpoint, &prepared)?;
        let prepared_commit = match decision {
            BatchCommitDecisionV1::Replay(checkpoint) => return Ok(checkpoint),
            BatchCommitDecisionV1::Commit(prepared) => prepared,
        };
        let ordinal = prepared_commit.batch_ordinal();
        let prepared_digest = prepared_commit.prepared_digest().clone();
        let checkpoint = prepared_commit.checkpoint().clone();
        let next = state.revision.next()?;
        let transaction = self.handle.begin_immediate().map_err(unavailable)?;
        require_revision(&transaction, &self.authority_namespace, state.revision)?;
        execute(
            &transaction,
            "INSERT INTO vector_authority_batches_v1 (
                 authority_namespace, build_id, batch_ordinal, request_digest, prepared_digest,
                 prior_checkpoint, committed_checkpoint, canonical_prepared
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            vec![
                text(&self.authority_namespace),
                text(&build_id.to_string()),
                u64_integer(ordinal, "batch ordinal")?,
                text(prepared.request.request_digest.as_str()),
                text(prepared_digest.as_str()),
                expected_checkpoint.map_or(Ok(ExactSqlValue::Null), blob_json)?,
                blob_json(&checkpoint)?,
                blob_json(&prepared)?,
            ],
        )?;
        let updated = execute(
            &transaction,
            "UPDATE vector_authority_stages_v1 SET checkpoint = ?3
             WHERE authority_namespace = ?1 AND build_id = ?2",
            vec![
                text(&self.authority_namespace),
                text(&build_id.to_string()),
                blob_json(&checkpoint)?,
            ],
        )?;
        if updated != 1 {
            return Err(corrupt(
                "durable vector stage disappeared before its checkpoint update",
            ));
        }
        update_head_pointers(
            &transaction,
            &self.authority_namespace,
            state.revision,
            next,
            state.authority.active_generation(),
            state.authority.rollback_generation(),
        )?;
        transaction.commit().map_err(unavailable)?;
        state.revision = next;
        match Arc::make_mut(&mut state.authority).apply_batch(build_id, prepared_commit) {
            Ok(applied) if applied == checkpoint => Ok(applied),
            Ok(_) => self.recover_after_committed_apply_failure(
                &mut state,
                "durable vector checkpoint differs from its prepared apply",
            ),
            Err(error) => self.recover_after_committed_apply_failure(
                &mut state,
                format!("durable vector batch could not apply after commit: {error}"),
            ),
        }
    }

    fn publish_generation_if_current(
        &self,
        build_id: &VectorGenerationBuildIdV1,
        expected_active: Option<&VectorGenerationIdV1>,
    ) -> Result<VectorGenerationPublicationV1, VectorAuthorityStoreErrorV1> {
        let mut state = self.lock()?;
        let mut candidate = (*state.authority).clone();
        let publication = candidate.publish_generation_if_current(build_id, expected_active)?;
        let generation = candidate
            .generation(&publication.generation_id)
            .ok_or_else(|| corrupt("published generation is absent from candidate state"))?;
        let next = state.revision.next()?;
        let transaction = self.handle.begin_immediate().map_err(unavailable)?;
        require_revision(&transaction, &self.authority_namespace, state.revision)?;
        persist_generation(&transaction, generation, &candidate)?;
        update_head_pointers(
            &transaction,
            &self.authority_namespace,
            state.revision,
            next,
            candidate.active_generation(),
            candidate.rollback_generation(),
        )?;
        let deleted = execute(
            &transaction,
            "DELETE FROM vector_authority_stages_v1
             WHERE authority_namespace = ?1 AND build_id = ?2",
            vec![text(&self.authority_namespace), text(&build_id.to_string())],
        )?;
        if deleted != 1 {
            return Err(corrupt(
                "durable vector stage disappeared before publication",
            ));
        }
        transaction.commit().map_err(unavailable)?;
        state.revision = next;
        state.staged_build_ids.remove(build_id);
        state.authority = Arc::new(candidate);
        Ok(publication)
    }

    fn activate_generation_if_current(
        &self,
        generation_id: &VectorGenerationIdV1,
        expected_active: Option<&VectorGenerationIdV1>,
    ) -> Result<Option<VectorGenerationIdV1>, VectorAuthorityStoreErrorV1> {
        let mut state = self.lock()?;
        let mut candidate = (*state.authority).clone();
        let previous = candidate.activate_generation_if_current(generation_id, expected_active)?;
        if previous.is_some() {
            persist_pointer_candidate(self, &mut state, candidate)?;
        }
        Ok(previous)
    }

    fn rollback_generation_if_current(
        &self,
        expected_active: Option<&VectorGenerationIdV1>,
    ) -> Result<VectorGenerationIdV1, VectorAuthorityStoreErrorV1> {
        let mut state = self.lock()?;
        let mut candidate = (*state.authority).clone();
        let active = candidate.rollback_generation_if_current(expected_active)?;
        persist_pointer_candidate(self, &mut state, candidate)?;
        Ok(active)
    }

    fn restore_active_generation_if_current(
        &self,
        expected_active: &VectorGenerationIdV1,
        replacement: Option<&VectorGenerationIdV1>,
    ) -> Result<(), VectorAuthorityStoreErrorV1> {
        let mut state = self.lock()?;
        let mut candidate = (*state.authority).clone();
        candidate.restore_active_generation_if_current(expected_active, replacement)?;
        persist_pointer_candidate(self, &mut state, candidate)
    }
}

fn persist_pointer_candidate(
    owner: &ProjectVectorAuthorityHandleV1,
    state: &mut OpenedVectorAuthorityV1,
    candidate: VectorGenerationAuthority,
) -> Result<(), VectorAuthorityStoreErrorV1> {
    let next = state.revision.next()?;
    let transaction = owner.handle.begin_immediate().map_err(unavailable)?;
    require_revision(&transaction, &owner.authority_namespace, state.revision)?;
    update_head_pointers(
        &transaction,
        &owner.authority_namespace,
        state.revision,
        next,
        candidate.active_generation(),
        candidate.rollback_generation(),
    )?;
    transaction.commit().map_err(unavailable)?;
    state.revision = next;
    state.authority = Arc::new(candidate);
    Ok(())
}

fn load_opened_state(
    transaction: &ExactSqlTransaction,
    authority_namespace: &str,
) -> Result<OpenedVectorAuthorityV1, VectorAuthorityStoreErrorV1> {
    let head = read_head(transaction, authority_namespace)?;
    let revision = head
        .as_ref()
        .map_or(VectorAuthorityRevisionV1::INITIAL, |head| head.revision);
    let stages = read_stages(transaction, authority_namespace)?;
    if head.is_none() && !stages.is_empty() {
        return Err(corrupt(
            "staged vector builds exist without an authority head",
        ));
    }
    let mut generation_ids = BTreeSet::new();
    if let Some(head) = head.as_ref() {
        generation_ids.extend(head.active.iter().cloned());
        generation_ids.extend(head.rollback.iter().cloned());
    }
    for stage in &stages {
        if let Some(base) = stage.plan.base_generation.as_ref() {
            generation_ids.insert(base.to_string());
        }
    }
    let mut generations = Vec::with_capacity(generation_ids.len());
    let mut blobs = BTreeMap::<String, (ContentDigest, Vec<f32>)>::new();
    for generation_id in generation_ids {
        let generation = read_generation(transaction, &generation_id)?.ok_or_else(|| {
            corrupt(format!(
                "head references missing generation {generation_id}"
            ))
        })?;
        for (digest, values) in read_generation_blobs(transaction, &generation_id)? {
            match blobs.entry(digest.to_string()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert((digest, values));
                }
                std::collections::btree_map::Entry::Occupied(entry) if entry.get().1 != values => {
                    return Err(corrupt(
                        "content-addressed vector blob has conflicting bytes",
                    ));
                }
                std::collections::btree_map::Entry::Occupied(_) => {}
            }
        }
        generations.push(generation);
    }
    let active = resolve_generation_id(
        head.as_ref().and_then(|head| head.active.as_deref()),
        &generations,
    )?;
    let rollback = resolve_generation_id(
        head.as_ref().and_then(|head| head.rollback.as_deref()),
        &generations,
    )?;
    let mut authority = VectorGenerationAuthority::restore_published(
        generations,
        blobs.into_values().collect(),
        active,
        rollback,
    )?;
    let mut staged_build_ids = BTreeSet::new();
    for stage in stages {
        let expected_build = stage.plan.build_id()?;
        if expected_build.to_string() != stage.build_id {
            return Err(corrupt("staged vector plan does not match its build id"));
        }
        if serde_json::to_string(&stage.plan.target_projection_key).map_err(serialization)?
            != stage.projection_key
        {
            return Err(corrupt(
                "staged vector plan does not match its projection key",
            ));
        }
        let build_id = authority.begin_generation(stage.plan)?;
        let mut expected_ordinal = 0_u64;
        for batch in read_batches(transaction, authority_namespace, &stage.build_id)? {
            if batch.ordinal != expected_ordinal {
                return Err(corrupt("staged vector batch ordinals are not contiguous"));
            }
            let actual = authority.commit_batch(
                &build_id,
                batch.prior_checkpoint.as_ref(),
                batch.prepared,
            )?;
            if actual != batch.committed_checkpoint {
                return Err(corrupt(
                    "staged vector checkpoint differs from batch replay",
                ));
            }
            expected_ordinal = expected_ordinal
                .checked_add(1)
                .ok_or_else(|| corrupt("staged vector batch ordinal overflow"))?;
        }
        if authority.staged_checkpoint(&build_id) != Some(&stage.checkpoint) {
            return Err(corrupt(
                "staged vector checkpoint differs from its stored head",
            ));
        }
        staged_build_ids.insert(build_id);
    }
    Ok(OpenedVectorAuthorityV1 {
        revision,
        authority: Arc::new(authority),
        staged_build_ids,
    })
}

struct StoredHead {
    revision: VectorAuthorityRevisionV1,
    active: Option<String>,
    rollback: Option<String>,
}

struct StoredStage {
    build_id: String,
    projection_key: String,
    plan: VectorGenerationPlanV1,
    checkpoint: VectorProjectionCheckpointV1,
}

struct StoredBatch {
    ordinal: u64,
    prior_checkpoint: Option<VectorProjectionCheckpointV1>,
    committed_checkpoint: VectorProjectionCheckpointV1,
    prepared: PreparedVectorGenerationV1,
}

fn read_head(
    transaction: &ExactSqlTransaction,
    authority_namespace: &str,
) -> Result<Option<StoredHead>, VectorAuthorityStoreErrorV1> {
    let rows = query(
        transaction,
        "SELECT revision, active_generation_id, rollback_generation_id
         FROM vector_authority_heads_v1 WHERE authority_namespace = ?1",
        vec![text(authority_namespace)],
    )?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    let [revision, active, rollback] = row.values.as_slice() else {
        return Err(invalid_shape("head"));
    };
    Ok(Some(StoredHead {
        revision: stored_revision(integer_value(revision, "revision")?)?,
        active: optional_text(active, "active generation")?,
        rollback: optional_text(rollback, "rollback generation")?,
    }))
}

fn read_stages(
    transaction: &ExactSqlTransaction,
    authority_namespace: &str,
) -> Result<Vec<StoredStage>, VectorAuthorityStoreErrorV1> {
    query(
        transaction,
        "SELECT build_id, projection_key, plan_digest, canonical_plan, checkpoint
         FROM vector_authority_stages_v1
         WHERE authority_namespace = ?1 ORDER BY build_id",
        vec![text(authority_namespace)],
    )?
    .into_iter()
    .map(|row| {
        let [
            ExactSqlValue::Text(build_id),
            ExactSqlValue::Text(projection_key),
            ExactSqlValue::Text(plan_digest),
            ExactSqlValue::Blob(plan),
            ExactSqlValue::Blob(checkpoint),
        ] = row.values.as_slice()
        else {
            return Err(invalid_shape("stage"));
        };
        let plan: VectorGenerationPlanV1 = decode_json(plan, "stage plan")?;
        if plan.identity_digest()?.as_str() != plan_digest {
            return Err(corrupt("staged vector plan digest mismatch"));
        }
        Ok(StoredStage {
            build_id: build_id.clone(),
            projection_key: projection_key.clone(),
            plan,
            checkpoint: decode_json(checkpoint, "stage checkpoint")?,
        })
    })
    .collect()
}

fn read_batches(
    transaction: &ExactSqlTransaction,
    authority_namespace: &str,
    build_id: &str,
) -> Result<Vec<StoredBatch>, VectorAuthorityStoreErrorV1> {
    query(
        transaction,
        "SELECT batch_ordinal, request_digest, prepared_digest, prior_checkpoint,
                committed_checkpoint, canonical_prepared
         FROM vector_authority_batches_v1
         WHERE authority_namespace = ?1 AND build_id = ?2 ORDER BY batch_ordinal",
        vec![text(authority_namespace), text(build_id)],
    )?
    .into_iter()
    .map(|row| {
        let [
            ordinal,
            ExactSqlValue::Text(request_digest),
            ExactSqlValue::Text(prepared_digest),
            prior,
            ExactSqlValue::Blob(checkpoint),
            ExactSqlValue::Blob(prepared),
        ] = row.values.as_slice()
        else {
            return Err(invalid_shape("batch"));
        };
        let prepared: PreparedVectorGenerationV1 = decode_json(prepared, "prepared batch")?;
        if prepared.request.request_digest.as_str() != request_digest {
            return Err(corrupt("prepared vector request digest mismatch"));
        }
        if prepared.prepared_digest()?.as_str() != prepared_digest {
            return Err(corrupt("prepared vector batch digest mismatch"));
        }
        Ok(StoredBatch {
            ordinal: stored_u64(integer_value(ordinal, "batch ordinal")?, "batch ordinal")?,
            prior_checkpoint: optional_json(prior, "prior checkpoint")?,
            committed_checkpoint: decode_json(checkpoint, "committed checkpoint")?,
            prepared,
        })
    })
    .collect()
}

fn read_generation(
    transaction: &ExactSqlTransaction,
    generation_id: &str,
) -> Result<Option<PublishedVectorGenerationV1>, VectorAuthorityStoreErrorV1> {
    let rows = query(
        transaction,
        "SELECT projection_key, manifest_digest, canonical_generation
         FROM vector_authority_generations_v1 WHERE generation_id = ?1",
        vec![text(generation_id)],
    )?;
    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };
    let [
        ExactSqlValue::Text(projection),
        ExactSqlValue::Text(manifest),
        ExactSqlValue::Blob(bytes),
    ] = row.values.as_slice()
    else {
        return Err(invalid_shape("generation"));
    };
    let generation: PublishedVectorGenerationV1 = decode_json(bytes, "published generation")?;
    if generation.generation_id().to_string() != generation_id
        || serde_json::to_string(generation.projection_key()).map_err(serialization)? != *projection
        || generation.manifest_digest().as_str() != manifest
    {
        return Err(corrupt("published vector generation metadata mismatch"));
    }
    Ok(Some(generation))
}

fn read_generation_blobs(
    transaction: &ExactSqlTransaction,
    generation_id: &str,
) -> Result<Vec<(ContentDigest, Vec<f32>)>, VectorAuthorityStoreErrorV1> {
    query(
        transaction,
        "SELECT b.output_digest, b.dimensions, b.canonical_f32_le
         FROM vector_authority_generation_blobs_v1 AS gb
         JOIN vector_authority_float_blobs_v1 AS b
           ON b.output_digest = gb.output_digest
         WHERE gb.generation_id = ?1 ORDER BY b.output_digest",
        vec![text(generation_id)],
    )?
    .into_iter()
    .map(|row| {
        let [
            ExactSqlValue::Text(digest),
            dimensions,
            ExactSqlValue::Blob(bytes),
        ] = row.values.as_slice()
        else {
            return Err(invalid_shape("float blob"));
        };
        let dimensions = stored_u64(
            integer_value(dimensions, "vector dimensions")?,
            "vector dimensions",
        )?;
        if dimensions == 0
            || usize::try_from(dimensions)
                .ok()
                .and_then(|length| length.checked_mul(4))
                != Some(bytes.len())
        {
            return Err(corrupt("stored vector dimensions do not match float bytes"));
        }
        let values = bytes
            .chunks_exact(4)
            .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect();
        Ok((
            ContentDigest::new(digest.clone())
                .map_err(|error| corrupt(format!("invalid stored vector digest: {error}")))?,
            values,
        ))
    })
    .collect()
}

fn persist_generation(
    transaction: &ExactSqlTransaction,
    generation: &PublishedVectorGenerationV1,
    authority: &VectorGenerationAuthority,
) -> Result<(), VectorAuthorityStoreErrorV1> {
    let generation_id = generation.generation_id().to_string();
    if let Some(existing) = read_generation(transaction, &generation_id)? {
        if existing != *generation {
            return Err(corrupt(
                "immutable vector generation conflicts with stored bytes",
            ));
        }
    } else {
        execute(
            transaction,
            "INSERT INTO vector_authority_generations_v1 (
                 generation_id, projection_key, manifest_digest, canonical_generation
             ) VALUES (?1, ?2, ?3, ?4)",
            vec![
                text(&generation_id),
                text(&serde_json::to_string(generation.projection_key()).map_err(serialization)?),
                text(generation.manifest_digest().as_str()),
                blob_json(generation)?,
            ],
        )?;
    }
    for row in generation.rows().values() {
        let values = authority
            .vector_content(&row.output_digest)
            .ok_or_else(|| {
                corrupt(format!(
                    "published generation is missing vector blob {}",
                    row.output_digest
                ))
            })?;
        persist_blob(transaction, &row.output_digest, values)?;
        execute(
            transaction,
            "INSERT OR IGNORE INTO vector_authority_generation_blobs_v1 (
                 generation_id, output_digest
             ) VALUES (?1, ?2)",
            vec![text(&generation_id), text(row.output_digest.as_str())],
        )?;
    }
    Ok(())
}

fn persist_blob(
    transaction: &ExactSqlTransaction,
    digest: &ContentDigest,
    values: &[f32],
) -> Result<(), VectorAuthorityStoreErrorV1> {
    let bytes = values
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect::<Vec<_>>();
    let rows = query(
        transaction,
        "SELECT dimensions, canonical_f32_le
         FROM vector_authority_float_blobs_v1 WHERE output_digest = ?1",
        vec![text(digest.as_str())],
    )?;
    if let Some(row) = rows.first() {
        let [dimensions, ExactSqlValue::Blob(existing)] = row.values.as_slice() else {
            return Err(invalid_shape("existing float blob"));
        };
        if stored_u64(
            integer_value(dimensions, "vector dimensions")?,
            "vector dimensions",
        )? != values.len() as u64
            || existing != &bytes
        {
            return Err(corrupt(
                "content-addressed vector blob conflicts with stored bytes",
            ));
        }
        return Ok(());
    }
    execute(
        transaction,
        "INSERT INTO vector_authority_float_blobs_v1 (
             output_digest, dimensions, byte_length, canonical_f32_le
         ) VALUES (?1, ?2, ?3, ?4)",
        vec![
            text(digest.as_str()),
            u64_integer(values.len() as u64, "vector dimensions")?,
            u64_integer(bytes.len() as u64, "vector byte length")?,
            ExactSqlValue::Blob(bytes),
        ],
    )?;
    Ok(())
}

fn ensure_head(
    transaction: &ExactSqlTransaction,
    authority_namespace: &str,
    revision: VectorAuthorityRevisionV1,
) -> Result<(), VectorAuthorityStoreErrorV1> {
    if revision != VectorAuthorityRevisionV1::INITIAL
        || read_head(transaction, authority_namespace)?.is_some()
    {
        return Ok(());
    }
    execute(
        transaction,
        "INSERT INTO vector_authority_heads_v1 (
             authority_namespace, revision, active_generation_id, rollback_generation_id
         ) VALUES (?1, 0, NULL, NULL)",
        vec![text(authority_namespace)],
    )?;
    Ok(())
}

fn require_revision(
    transaction: &ExactSqlTransaction,
    authority_namespace: &str,
    expected: VectorAuthorityRevisionV1,
) -> Result<(), VectorAuthorityStoreErrorV1> {
    let actual = read_head(transaction, authority_namespace)?
        .map_or(VectorAuthorityRevisionV1::INITIAL, |head| head.revision);
    if actual == expected {
        return Ok(());
    }
    Err(VectorAuthorityStoreErrorV1::Conflict {
        expected: expected.get(),
        actual: actual.get(),
    })
}

fn update_head_pointers(
    transaction: &ExactSqlTransaction,
    authority_namespace: &str,
    expected: VectorAuthorityRevisionV1,
    next: VectorAuthorityRevisionV1,
    active: Option<&VectorGenerationIdV1>,
    rollback: Option<&VectorGenerationIdV1>,
) -> Result<(), VectorAuthorityStoreErrorV1> {
    let changed = execute(
        transaction,
        "UPDATE vector_authority_heads_v1
         SET revision = ?1, active_generation_id = ?2, rollback_generation_id = ?3
         WHERE authority_namespace = ?4 AND revision = ?5",
        vec![
            revision_integer(next)?,
            optional_generation(active),
            optional_generation(rollback),
            text(authority_namespace),
            revision_integer(expected)?,
        ],
    )?;
    if changed != 1 {
        return Err(corrupt(
            "vector-authority CAS lost its immediate transaction authority",
        ));
    }
    Ok(())
}

fn resolve_generation_id(
    stored: Option<&str>,
    generations: &[PublishedVectorGenerationV1],
) -> Result<Option<VectorGenerationIdV1>, VectorAuthorityStoreErrorV1> {
    stored
        .map(|stored| {
            generations
                .iter()
                .find(|generation| generation.generation_id().to_string() == stored)
                .map(|generation| generation.generation_id().clone())
                .ok_or_else(|| corrupt(format!("pointer references missing generation {stored}")))
        })
        .transpose()
}

fn execute(
    transaction: &ExactSqlTransaction,
    sql: &str,
    values: Vec<ExactSqlValue>,
) -> Result<usize, VectorAuthorityStoreErrorV1> {
    transaction
        .execute(ExactSqlStatement::new(sql.to_owned(), values).map_err(unavailable)?)
        .map(|receipt| receipt.changed_rows)
        .map_err(unavailable)
}

fn query(
    transaction: &ExactSqlTransaction,
    sql: &str,
    values: Vec<ExactSqlValue>,
) -> Result<Vec<ExactSqlRow>, VectorAuthorityStoreErrorV1> {
    transaction
        .query(ExactSqlStatement::new(sql.to_owned(), values).map_err(unavailable)?)
        .map(|rows| rows.rows)
        .map_err(unavailable)
}

fn text(value: &str) -> ExactSqlValue {
    ExactSqlValue::Text(value.to_owned())
}

fn blob_json(value: &impl serde::Serialize) -> Result<ExactSqlValue, VectorAuthorityStoreErrorV1> {
    serde_json::to_vec(value)
        .map(ExactSqlValue::Blob)
        .map_err(serialization)
}

fn optional_generation(value: Option<&VectorGenerationIdV1>) -> ExactSqlValue {
    value.map_or(ExactSqlValue::Null, |value| text(&value.to_string()))
}

fn revision_integer(
    revision: VectorAuthorityRevisionV1,
) -> Result<ExactSqlValue, VectorAuthorityStoreErrorV1> {
    u64_integer(revision.get(), "revision")
}

fn u64_integer(value: u64, label: &str) -> Result<ExactSqlValue, VectorAuthorityStoreErrorV1> {
    i64::try_from(value)
        .map(ExactSqlValue::Integer)
        .map_err(|_| corrupt(format!("{label} exceeds SQLite integer range")))
}

fn stored_revision(value: i64) -> Result<VectorAuthorityRevisionV1, VectorAuthorityStoreErrorV1> {
    Ok(VectorAuthorityRevisionV1::from_stored(stored_u64(
        value, "revision",
    )?))
}

fn stored_u64(value: i64, label: &str) -> Result<u64, VectorAuthorityStoreErrorV1> {
    u64::try_from(value).map_err(|_| corrupt(format!("stored {label} is negative")))
}

fn integer_value(value: &ExactSqlValue, label: &str) -> Result<i64, VectorAuthorityStoreErrorV1> {
    match value {
        ExactSqlValue::Integer(value) => Ok(*value),
        _ => Err(invalid_shape(label)),
    }
}

fn optional_text(
    value: &ExactSqlValue,
    label: &str,
) -> Result<Option<String>, VectorAuthorityStoreErrorV1> {
    match value {
        ExactSqlValue::Null => Ok(None),
        ExactSqlValue::Text(value) => Ok(Some(value.clone())),
        _ => Err(invalid_shape(label)),
    }
}

fn optional_json<T: serde::de::DeserializeOwned>(
    value: &ExactSqlValue,
    label: &str,
) -> Result<Option<T>, VectorAuthorityStoreErrorV1> {
    match value {
        ExactSqlValue::Null => Ok(None),
        ExactSqlValue::Blob(value) => decode_json(value, label).map(Some),
        _ => Err(invalid_shape(label)),
    }
}

fn decode_json<T: serde::de::DeserializeOwned>(
    value: &[u8],
    label: &str,
) -> Result<T, VectorAuthorityStoreErrorV1> {
    serde_json::from_slice(value).map_err(|error| corrupt(format!("invalid {label}: {error}")))
}

fn unavailable(error: impl std::fmt::Display) -> VectorAuthorityStoreErrorV1 {
    VectorAuthorityStoreErrorV1::Unavailable(error.to_string())
}

fn serialization(error: impl std::fmt::Display) -> VectorAuthorityStoreErrorV1 {
    VectorAuthorityStoreErrorV1::Unavailable(error.to_string())
}

fn corrupt(message: impl Into<String>) -> VectorAuthorityStoreErrorV1 {
    VectorAuthorityStoreErrorV1::Corrupt(message.into())
}

fn invalid_shape(label: &str) -> VectorAuthorityStoreErrorV1 {
    corrupt(format!("stored {label} row has an invalid shape"))
}

#[cfg(test)]
#[path = "vector_authority/tests.rs"]
mod tests;
