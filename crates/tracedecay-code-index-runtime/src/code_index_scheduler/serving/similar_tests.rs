use super::*;

struct TestControl {
    cancelled: bool,
    deadline_exceeded: bool,
}

impl CodeIndexExecutionControlV1 for TestControl {
    fn is_cancelled(&self) -> bool {
        self.cancelled
    }

    fn is_deadline_exceeded(&self) -> bool {
        self.deadline_exceeded
    }
}

#[test]
fn near_serving_checks_cancellation_before_deadline() {
    let cancelled = TestControl {
        cancelled: true,
        deadline_exceeded: true,
    };
    assert_eq!(
        check_similar_control(&cancelled),
        Err(RetrievalPortError::Cancelled)
    );
}

#[test]
fn near_serving_maps_deadline_to_bounded_work_failure() {
    let deadline = TestControl {
        cancelled: false,
        deadline_exceeded: true,
    };
    assert_eq!(
        check_similar_control(&deadline),
        Err(RetrievalPortError::BudgetExceeded)
    );
}

#[test]
fn exhausted_near_page_reports_capped_verification_coverage() {
    let coverage = similar_near_partial_coverage();

    assert_eq!(coverage.capped, 1);
    assert_eq!(coverage.examined, 0);
    assert_eq!(
        similar_near_budget_reason(),
        vec![CloneFingerprintPartialReasonV1::VerificationWorkBudget]
    );
}

#[test]
fn fresh_near_lane_keeps_result_and_lookahead_work_slots() {
    assert_eq!(similar_exact_lane_budget(1, 3, true), (0, 0));
    assert_eq!(similar_exact_lane_budget(4, 8, true), (3, 5));
    assert_eq!(similar_exact_lane_budget(4, 8, false), (4, 7));
}

#[test]
fn exact_lane_budget_is_saturating_at_small_request_limits() {
    assert_eq!(similar_exact_lane_budget(0, 0, true), (0, 0));
    assert_eq!(similar_exact_lane_budget(1, 1, true), (0, 0));
    assert_eq!(similar_exact_lane_budget(1, 2, true), (0, 0));
}

fn test_source(
    eligibility: tracedecay_code_index::clones::CloneBodyEligibilityV1,
) -> tracedecay_code_index::clones::CodeIndexCloneBodyV1 {
    let tokens = (0..14)
        .map(
            |ordinal| tracedecay_code_index::clones::ConservativeCloneTokenV1::Syntax {
                syntax_kind: "identifier".to_owned(),
                text: format!("token{ordinal}"),
            },
        )
        .collect::<Vec<_>>();
    let extracted = tracedecay_code_extraction::ExtractedCloneBodyV1 {
        logical_path: "src/lib.rs".to_owned(),
        language: "rust".to_owned(),
        symbol_kind: tracedecay_domain::NodeKind::Function,
        symbol_occurrence_id: "symbol.fixture".to_owned(),
        body_span: tracedecay_domain::SourceSpan {
            start_byte: 0,
            end_byte: 64,
        },
        normalization_revision: 1,
        non_trivia_token_count: tokens.len() as u32,
        eligibility,
        tokenization_status: tracedecay_code_extraction::CloneBodyTokenizationStatusV1::Complete,
        tokenization_issues: Vec::new(),
        conservative_tokens: tokens,
        rename_normalization_revision: None,
        rename_status: tracedecay_code_extraction::CloneBodyRenameStatusV1::UnsupportedLanguage,
        rename_issues: Vec::new(),
        rename_tokens: None,
    };
    let payload = std::sync::Arc::new(
        tracedecay_code_index::clones::CloneBodyPayloadV1::from_extracted(&extracted)
            .expect("valid test clone payload"),
    );
    tracedecay_code_index::clones::CodeIndexCloneBodyV1 {
        occurrence: tracedecay_code_index::clones::CloneBodyOccurrenceV1 {
            project_id: tracedecay_domain::ProjectId::new("project.fixture").expect("project id"),
            repository_id: tracedecay_domain::RepositoryId::new("repository.fixture")
                .expect("repository id"),
            worktree_id: None,
            source_generation: tracedecay_domain::CodeGenerationId::new("generation.fixture")
                .expect("generation id"),
            snapshot_digest: ManifestDigest::new(format!("sha256:{}", "a".repeat(64)))
                .expect("snapshot digest"),
            symbol_occurrence_id: tracedecay_domain::SymbolOccurrenceId::new("symbol.fixture")
                .expect("symbol id"),
            path: "src/lib.rs".to_owned(),
            body_span: tracedecay_domain::SourceSpan {
                start_byte: 0,
                end_byte: 64,
            },
            payload_digest: payload.payload_digest.clone(),
            eligibility,
        },
        payload,
    }
}

#[test]
fn near_route_reservation_requires_the_requested_source_stream() {
    let eligible = test_source(tracedecay_code_index::clones::CloneBodyEligibilityV1::Eligible);
    let whole = tracedecay_query::code_search::CodeIndexSimilarSourceExtentV1::WholeBody;
    let conservative = [tracedecay_code_index::clones::CloneNormalizationClassV1::Conservative];
    let rename = [tracedecay_code_index::clones::CloneNormalizationClassV1::Rename];

    assert!(similar_near_route_is_available(
        &eligible,
        &whole,
        &conservative
    ));
    assert!(!similar_near_route_is_available(&eligible, &whole, &rename));
    assert!(!similar_near_route_is_available(
        &test_source(
            tracedecay_code_index::clones::CloneBodyEligibilityV1::ExcludedTooSmall {
                minimum_tokens: 14,
            },
        ),
        &whole,
        &conservative,
    ));
}

#[test]
fn selected_near_descriptor_is_distinct_from_whole_body_descriptor() {
    let source = test_source(tracedecay_code_index::clones::CloneBodyEligibilityV1::Eligible);
    let selected =
        CloneSelectedBlockV1::from_payload(&source.payload, source.occurrence.eligibility, 0..14)
            .expect("selected test block");
    let artifact =
        ManifestDigest::new(format!("sha256:{}", "b".repeat(64))).expect("artifact digest");
    let whole = similar_near_query_descriptor(&source, &artifact, None)
        .expect("whole descriptor")
        .expect("whole stream");
    let selected = similar_near_query_descriptor(&source, &artifact, Some(&selected))
        .expect("selected descriptor")
        .expect("selected stream");

    assert_ne!(whole, selected);
}

#[test]
fn exhausted_near_page_preserves_near_continuation_value() {
    let source = test_source(tracedecay_code_index::clones::CloneBodyEligibilityV1::Eligible);
    let cursor = "ccclone2.authenticated-near-cursor".to_owned();

    let read = similar_near_budget_exhausted_whole_body(&source, Some(cursor.clone()));
    let tracedecay_query::code_search::CodeIndexSimilarNearReadV1::WholeBody(read) = read else {
        panic!("expected whole-body near read");
    };
    assert_eq!(read.page.next_cursor, Some(cursor));
}
