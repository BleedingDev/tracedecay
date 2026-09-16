use std::path::{Component, Path};

use tracedecay_code_index::clones::{
    CloneExactKeyV1, CloneNormalizationClassV1, CodeIndexCloneBodyV1,
};
use tracedecay_code_index::production::{CodeIndexExecutionControlV1, CodeIndexInterruptionV1};
use tracedecay_contracts::retrieval::{
    RedundancyCoverageV1, RedundancyFamilyV1, RedundancyPartialReasonV1, RedundancyRankingV1,
    RedundancyResultV1, SimilarFamilyV1, SimilarMatchClassV1, SimilarOccurrenceV1,
};
use tracedecay_domain::canonical_sha256;
use tracedecay_query::code_search::{CodeIndexRedundancyQueryV1, CodeIndexRedundancyScopeV1};
use tracedecay_query::retrieval::lexical::{
    CloneArtifactCursorV1, CloneExactArtifactMemberV1, CodeLexicalArtifactErrorV1,
};

use super::ProductionCodeIndexQueryOwnersV1;
use crate::query::retrieval::ports::RetrievalPortError;

const REDUNDANCY_MEMBER_CURSOR_PREFIX_V1: &str = "tracedecay.redundancy-member.v1:";

/// A redundancy result cursor has to resume two ordered streams at once:
/// the family stream and the member stream inside the family that was
/// truncated. Keeping the family cursor from before that family prevents a
/// member continuation from being passed to the family reader (which would
/// either skip the family or reject the cursor as the wrong artifact kind).
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct RedundancyMemberContinuationCursorV1 {
    family_cursor: Option<String>,
    family_key: CloneExactKeyV1,
    member_cursor: CloneArtifactCursorV1,
}

impl RedundancyMemberContinuationCursorV1 {
    fn encode(&self) -> Result<String, RetrievalPortError> {
        serde_json::to_string(self)
            .map(|payload| format!("{REDUNDANCY_MEMBER_CURSOR_PREFIX_V1}{payload}"))
            .map_err(|error| {
                RetrievalPortError::Contract(format!(
                    "failed to encode redundancy member cursor: {error}"
                ))
            })
    }

    fn decode(cursor: &str) -> Result<Self, RetrievalPortError> {
        let payload = cursor
            .strip_prefix(REDUNDANCY_MEMBER_CURSOR_PREFIX_V1)
            .ok_or_else(|| {
                RetrievalPortError::Contract(
                    "redundancy member cursor has an unknown encoding".to_owned(),
                )
            })?;
        serde_json::from_str(payload).map_err(|error| {
            RetrievalPortError::Contract(format!("invalid redundancy member cursor: {error}"))
        })
    }
}

fn decode_redundancy_cursor(
    cursor: Option<&str>,
) -> Result<(Option<String>, Option<RedundancyMemberContinuationCursorV1>), RetrievalPortError> {
    let Some(cursor) = cursor else {
        return Ok((None, None));
    };
    if cursor.starts_with(REDUNDANCY_MEMBER_CURSOR_PREFIX_V1) {
        let continuation = RedundancyMemberContinuationCursorV1::decode(cursor)?;
        return Ok((continuation.family_cursor.clone(), Some(continuation)));
    }
    // Cursors minted before member continuation was introduced remain valid
    // family cursors and can continue paging from the family reader directly.
    Ok((Some(cursor.to_owned()), None))
}

fn redundancy_checkpoint(
    control: &dyn CodeIndexExecutionControlV1,
) -> Result<(), RetrievalPortError> {
    if control.is_cancelled() || control.is_deadline_exceeded() {
        Err(RetrievalPortError::Cancelled)
    } else {
        Ok(())
    }
}

impl ProductionCodeIndexQueryOwnersV1 {
    pub(crate) fn redundancy(
        &self,
        request: &CodeIndexRedundancyQueryV1,
        control: &dyn CodeIndexExecutionControlV1,
    ) -> Result<RedundancyResultV1, RetrievalPortError> {
        redundancy_checkpoint(control)?;
        let family_page_limit = request.family_limit.min(request.work_limit / 3).max(1);
        let (family_cursor, mut member_continuation) =
            decode_redundancy_cursor(request.cursor.as_deref())?;
        let pull_request_scope_digest = match &request.scope {
            CodeIndexRedundancyScopeV1::PullRequest {
                provider,
                pull_request_id,
                head_commit_id,
                changed_paths,
            } => Some(
                canonical_sha256(&(
                    "tracedecay.redundancy.pull-request-scope.v1",
                    provider,
                    pull_request_id,
                    head_commit_id,
                    changed_paths,
                ))
                .map_err(|error| RetrievalPortError::Contract(error.to_string()))?,
            ),
            CodeIndexRedundancyScopeV1::Repository | CodeIndexRedundancyScopeV1::Path(_) => None,
        };
        let (path, pull_request_paths) = match &request.scope {
            CodeIndexRedundancyScopeV1::Repository => (None, None),
            CodeIndexRedundancyScopeV1::Path(path) => (Some(path.as_str()), None),
            CodeIndexRedundancyScopeV1::PullRequest { changed_paths, .. } => {
                (None, Some(changed_paths.as_slice()))
            }
        };
        let page = self
            .hydration
            .clone_exact_family_page(
                &request.project_id,
                &request.repository_id,
                &request.match_classes,
                path,
                pull_request_paths,
                pull_request_scope_digest.as_ref(),
                request.include_generated_paths,
                family_cursor.as_deref(),
                family_page_limit,
                control,
            )
            .map_err(redundancy_artifact_error)?;
        if member_continuation.is_some() && page.families.is_empty() {
            return Err(RetrievalPortError::StaleEvidence);
        }
        let page_continuation = page.next_cursor.clone();
        let mut families = Vec::with_capacity(page.families.len());
        let mut examined_families = 0usize;
        let mut examined_members = 0usize;
        let mut work_spent = 0usize;
        let mut work_exhausted = false;
        let mut member_pagination_incomplete = false;
        let mut report_continuation = family_cursor.clone();
        let mut candidate_family_cursor = family_cursor;
        for candidate in page.families {
            redundancy_checkpoint(control)?;
            if work_spent.saturating_add(3) > request.work_limit {
                work_exhausted = true;
                break;
            }
            if let Some(continuation) = member_continuation.as_ref() {
                if continuation.family_key != candidate.key {
                    return Err(RetrievalPortError::StaleEvidence);
                }
            }
            examined_families = examined_families.saturating_add(1);
            work_spent = work_spent.saturating_add(1);
            let source = self
                .hydration
                .clone_body(&candidate.representative)
                .map_err(|error| RetrievalPortError::AuthorityUnavailable(error.to_string()))?
                .ok_or_else(|| {
                    RetrievalPortError::AuthorityUnavailable(
                        "clone family representative is unavailable".to_owned(),
                    )
                })?;
            if source.occurrence.project_id != request.project_id
                || source.occurrence.repository_id != request.repository_id
            {
                return Err(RetrievalPortError::AuthorityUnavailable(
                    "clone family representative is outside the authorized repository".to_owned(),
                ));
            }
            work_spent = work_spent.saturating_add(1);
            examined_members = examined_members.saturating_add(1);
            let remaining_work = request.work_limit.saturating_sub(work_spent);
            let member_cursor = member_continuation
                .as_ref()
                .map(|continuation| &continuation.member_cursor);
            let read = self.verified_redundancy_members(
                &source,
                &candidate.key,
                path,
                request.include_generated_paths,
                request.member_limit.saturating_sub(1),
                remaining_work,
                member_cursor,
                control,
            )?;
            work_spent = work_spent.saturating_add(read.work_spent);
            examined_members = examined_members.saturating_add(read.work_spent);
            let mut members = Vec::with_capacity(read.members.len().saturating_add(1));
            members.push(similar_occurrence(&source.occurrence));
            members.extend(
                read.members
                    .iter()
                    .map(|member| similar_occurrence(&member.occurrence)),
            );
            work_exhausted |= read.work_exhausted;
            let match_class = match candidate.key.class {
                CloneNormalizationClassV1::Conservative => SimilarMatchClassV1::ConservativeExact,
                CloneNormalizationClassV1::Rename => SimilarMatchClassV1::RenameNormalizedExact,
            };
            let next_cursor = read
                .next_cursor
                .as_ref()
                .map(CloneArtifactCursorV1::encode)
                .transpose()
                .map_err(|error| RetrievalPortError::AuthorityUnavailable(error.to_string()))?;
            let complete = read.complete && members.len() == candidate.member_count;
            let generated_members = members
                .iter()
                .filter(|member| is_generated_path(&member.path))
                .map(|member| member.symbol_occurrence_id.clone())
                .collect();
            families.push(RedundancyFamilyV1 {
                family: SimilarFamilyV1 {
                    match_class,
                    normalization_revision: candidate.key.normalization_revision,
                    family_digest: candidate.key.digest.clone(),
                    representative_payload_digest: source.payload.payload_digest.clone(),
                    member_count: members.len(),
                    members,
                    complete,
                    next_cursor,
                },
                total_member_count: candidate.member_count,
                reviewable_source_bytes: candidate.reviewable_source_bytes,
                generated_members,
            });
            if !read.complete {
                member_pagination_incomplete = true;
                let member_cursor = read.next_cursor.clone().ok_or_else(|| {
                    RetrievalPortError::AuthorityUnavailable(
                        "incomplete clone family page has no member continuation".to_owned(),
                    )
                })?;
                report_continuation = Some(
                    RedundancyMemberContinuationCursorV1 {
                        family_cursor: candidate_family_cursor.clone(),
                        family_key: candidate.key,
                        member_cursor,
                    }
                    .encode()?,
                );
                break;
            }
            report_continuation = Some(candidate.continuation.clone());
            candidate_family_cursor = Some(candidate.continuation);
            member_continuation = None;
            if work_exhausted {
                break;
            }
        }
        let source_generation = self.hydration.metadata().generation.clone();
        let (coverage, next_cursor) = redundancy_coverage(
            work_exhausted,
            family_page_limit < request.family_limit,
            member_pagination_incomplete,
            page_continuation,
            report_continuation,
            examined_families,
            examined_members,
        );
        Ok(RedundancyResultV1 {
            source_generation,
            ranked_by: RedundancyRankingV1::ReviewableSourceBytes,
            families,
            coverage,
            next_cursor,
        })
    }

    fn verified_redundancy_members(
        &self,
        source: &CodeIndexCloneBodyV1,
        key: &CloneExactKeyV1,
        path: Option<&str>,
        include_generated_paths: bool,
        result_limit: usize,
        work_limit: usize,
        start_cursor: Option<&CloneArtifactCursorV1>,
        control: &dyn CodeIndexExecutionControlV1,
    ) -> Result<RedundancyMemberReadV1, RetrievalPortError> {
        let mut members = Vec::new();
        let mut cursor = start_cursor.cloned();
        let mut work_spent = 0usize;
        loop {
            redundancy_checkpoint(control)?;
            if members.len() >= result_limit {
                return Ok(RedundancyMemberReadV1 {
                    members,
                    complete: false,
                    next_cursor: cursor,
                    work_spent,
                    work_exhausted: false,
                });
            }
            if work_spent >= work_limit {
                return Ok(RedundancyMemberReadV1 {
                    members,
                    complete: false,
                    next_cursor: cursor,
                    work_spent,
                    work_exhausted: true,
                });
            }
            let page_limit = result_limit
                .saturating_sub(members.len())
                .min(work_limit.saturating_sub(work_spent));
            let page = self
                .hydration
                .clone_exact_page(
                    &source.occurrence,
                    key,
                    cursor.as_ref(),
                    page_limit,
                    control,
                )
                .map_err(redundancy_artifact_error)?;
            work_spent = work_spent.saturating_add(page.members.len());
            for member in page.members {
                if report_path_matches(&member.occurrence.path, path, include_generated_paths)
                    && tracedecay_code_index::clones::verify_exact_clone_payload(
                        &source.payload,
                        &member.payload,
                        key,
                    )
                {
                    members.push(member);
                }
            }
            match page.next_cursor {
                Some(next) => cursor = Some(next),
                None => {
                    return Ok(RedundancyMemberReadV1 {
                        members,
                        complete: true,
                        next_cursor: None,
                        work_spent,
                        work_exhausted: false,
                    });
                }
            }
        }
    }
}

fn redundancy_artifact_error(error: CodeLexicalArtifactErrorV1) -> RetrievalPortError {
    match error {
        CodeLexicalArtifactErrorV1::Contract(message)
            if matches!(
                message.as_str(),
                "clone family cursor does not match its artifact or request"
                    | "clone exact cursor does not match its artifact, key, or authority"
            ) =>
        {
            RetrievalPortError::StaleEvidence
        }
        CodeLexicalArtifactErrorV1::Interrupted(CodeIndexInterruptionV1::Cancelled)
        | CodeLexicalArtifactErrorV1::Interrupted(CodeIndexInterruptionV1::DeadlineExceeded) => {
            RetrievalPortError::Cancelled
        }
        error => RetrievalPortError::AuthorityUnavailable(error.to_string()),
    }
}

struct RedundancyMemberReadV1 {
    members: Vec<CloneExactArtifactMemberV1>,
    complete: bool,
    next_cursor: Option<CloneArtifactCursorV1>,
    work_spent: usize,
    work_exhausted: bool,
}

fn report_path_matches(path: &str, scope: Option<&str>, include_generated_paths: bool) -> bool {
    tracedecay_domain::repository_path_matches_scope(path, scope)
        && (include_generated_paths || !is_generated_path(path))
}

fn is_generated_path(path: &str) -> bool {
    Path::new(path).components().any(|component| {
        matches!(
            component,
            Component::Normal(segment)
                if segment
                    .to_str()
                    .is_some_and(tracedecay_domain::is_generated_dir_segment)
        )
    })
}

fn similar_occurrence(
    occurrence: &tracedecay_code_index::clones::CloneBodyOccurrenceV1,
) -> SimilarOccurrenceV1 {
    SimilarOccurrenceV1 {
        project_id: occurrence.project_id.clone(),
        repository_id: occurrence.repository_id.clone(),
        worktree_id: occurrence.worktree_id.clone(),
        source_generation: occurrence.source_generation.clone(),
        snapshot_digest: occurrence.snapshot_digest.clone(),
        symbol_occurrence_id: occurrence.symbol_occurrence_id.clone(),
        path: occurrence.path.clone(),
        body_span: occurrence.body_span,
    }
}

fn redundancy_coverage(
    work_exhausted: bool,
    family_budget_limited: bool,
    member_pagination_incomplete: bool,
    page_continuation: Option<String>,
    report_continuation: Option<String>,
    examined_families: usize,
    examined_members: usize,
) -> (RedundancyCoverageV1, Option<String>) {
    // The public V1 reason set has no member-limit variant. A bounded member
    // page therefore uses WorkLimit, while its composite cursor still tells
    // the next request exactly which family and member row to resume.
    if work_exhausted
        || member_pagination_incomplete
        || (family_budget_limited && page_continuation.is_some())
    {
        (
            RedundancyCoverageV1::Partial {
                reason: RedundancyPartialReasonV1::WorkLimit,
                examined_families,
                examined_members,
            },
            report_continuation,
        )
    } else if page_continuation.is_some() {
        (
            RedundancyCoverageV1::Partial {
                reason: RedundancyPartialReasonV1::FamilyLimit,
                examined_families,
                examined_members,
            },
            page_continuation,
        )
    } else {
        (
            RedundancyCoverageV1::Complete {
                examined_families,
                examined_members,
            },
            None,
        )
    }
}

#[cfg(test)]
#[path = "family_report_tests.rs"]
mod tests;
