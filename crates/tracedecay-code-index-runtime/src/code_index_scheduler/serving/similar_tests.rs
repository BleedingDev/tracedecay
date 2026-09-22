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
fn exact_lane_can_fill_the_request_before_the_near_phase() {
    assert_eq!(similar_exact_lane_budget(1, 3), (1, 2));
    assert_eq!(similar_exact_lane_budget(4, 8), (4, 7));
}

#[test]
fn exact_lane_budget_is_saturating_at_small_request_limits() {
    assert_eq!(similar_exact_lane_budget(0, 0), (0, 0));
    assert_eq!(similar_exact_lane_budget(1, 1), (1, 0));
    assert_eq!(similar_exact_lane_budget(1, 2), (1, 1));
}

#[test]
fn exact_page_work_budget_includes_the_authenticated_lookahead_row() {
    assert_eq!(exact_page_work_spent(4, true), 5);
    assert_eq!(exact_page_work_spent(4, false), 4);
    assert_eq!(exact_page_work_spent(0, true), 1);
}

#[test]
fn exact_page_limit_reserves_lookahead_before_the_reader_runs() {
    assert_eq!(exact_page_read_limit(5), 4);
    assert_eq!(exact_page_read_limit(1), 0);
    assert_eq!(exact_page_read_limit(0), 0);
}

fn test_source(
    eligibility: tracedecay_code_index::clones::CloneBodyEligibilityV1,
) -> tracedecay_code_index::clones::CodeIndexCloneBodyV1 {
    let tokens = (0..28)
        .map(
            |ordinal| tracedecay_code_index::clones::ConservativeCloneTokenV1::Syntax {
                syntax_kind: "identifier".to_owned(),
                text: format!("token{}", ordinal % 14),
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
fn near_route_requires_the_requested_source_stream() {
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
fn selected_near_descriptor_binds_equal_slices_to_their_source_offsets() {
    let source = test_source(tracedecay_code_index::clones::CloneBodyEligibilityV1::Eligible);
    let first =
        CloneSelectedBlockV1::from_payload(&source.payload, source.occurrence.eligibility, 0..14)
            .expect("first selected test block");
    let second =
        CloneSelectedBlockV1::from_payload(&source.payload, source.occurrence.eligibility, 14..28)
            .expect("second selected test block");
    assert_eq!(first.tokens(), second.tokens());
    let artifact =
        ManifestDigest::new(format!("sha256:{}", "b".repeat(64))).expect("artifact digest");
    let first_descriptor = similar_near_query_descriptor(&source, &artifact, Some(&first))
        .expect("first descriptor")
        .expect("first stream");
    let second_descriptor = similar_near_query_descriptor(&source, &artifact, Some(&second))
        .expect("second descriptor")
        .expect("second stream");

    assert_ne!(
        first_descriptor, second_descriptor,
        "equal selected token slices at different offsets must not share a cursor"
    );
}

#[test]
fn similar_cursor_descriptor_binds_all_classes_and_the_source_extent() {
    let source = test_source(tracedecay_code_index::clones::CloneBodyEligibilityV1::Eligible);
    let artifact =
        ManifestDigest::new(format!("sha256:{}", "b".repeat(64))).expect("artifact digest");
    let mut request = tracedecay_query::code_search::CodeIndexSimilarRequestV1 {
        project_root: std::path::PathBuf::new(),
        target: tracedecay_query::code_search::CodeIndexSimilarTargetV1::SymbolOccurrence(
            source.occurrence.symbol_occurrence_id.clone(),
        ),
        source_extent: tracedecay_query::code_search::CodeIndexSimilarSourceExtentV1::WholeBody,
        match_classes: vec![tracedecay_code_index::clones::CloneNormalizationClassV1::Conservative],
        result_limit: 1,
        work_limit: 4,
        cursor: None,
        authority: None,
        deadline: None,
        cancellation: None,
    };
    let conservative =
        similar_query_descriptor(&request, &source, &artifact).expect("conservative descriptor");

    request
        .match_classes
        .push(tracedecay_code_index::clones::CloneNormalizationClassV1::Rename);
    let both_classes =
        similar_query_descriptor(&request, &source, &artifact).expect("both-class descriptor");
    assert_ne!(conservative, both_classes);

    request.match_classes =
        vec![tracedecay_code_index::clones::CloneNormalizationClassV1::Conservative];
    request.source_extent =
        tracedecay_query::code_search::CodeIndexSimilarSourceExtentV1::SelectedTokenRange {
            start: 1,
            end: 8,
        };
    let selected =
        similar_query_descriptor(&request, &source, &artifact).expect("selected descriptor");
    assert_ne!(conservative, selected);
}

#[test]
fn final_empty_partial_near_page_has_no_synthetic_continuation() {
    let source = test_source(tracedecay_code_index::clones::CloneBodyEligibilityV1::Eligible);

    let read = similar_near_budget_exhausted_whole_body(&source);
    let tracedecay_query::code_search::CodeIndexSimilarNearReadV1::WholeBody(read) = read else {
        panic!("expected whole-body near read");
    };
    assert!(read.page.members.is_empty());
    assert!(read.page.next_cursor.is_none());
}

#[test]
fn exact_filled_page_advances_once_into_the_near_phase() {
    let source = test_source(tracedecay_code_index::clones::CloneBodyEligibilityV1::Eligible);
    let read = similar_near_budget_exhausted_whole_body(&source);

    assert!(similar_near_phase_cursor_needed(
        false,
        true,
        true,
        true,
        similar_near_read_is_partial_without_cursor(&read),
    ));
    assert!(!similar_near_phase_cursor_needed(
        true,
        true,
        true,
        true,
        similar_near_read_is_partial_without_cursor(&read),
    ));
}

#[test]
fn near_phase_completion_does_not_mint_another_cursor() {
    let source = test_source(tracedecay_code_index::clones::CloneBodyEligibilityV1::Eligible);
    let mut read = similar_near_budget_exhausted_whole_body(&source);
    set_similar_near_next_cursor(&mut read, "ccclone2.near-phase".to_owned());
    assert!(!similar_near_read_is_partial_without_cursor(&read));
    assert!(!similar_near_phase_cursor_needed(
        true, true, true, true, true,
    ));
    assert!(!similar_near_phase_cursor_needed(
        false, false, false, true, true,
    ));
}
