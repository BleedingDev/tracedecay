use tracedecay_query::search_quality::semantic_ablation::{
    SEMANTIC_ABLATION_REPETITIONS, compute_semantic_corpus_digest, packaged,
    validate_semantic_ablation_inputs, validate_semantic_model_manifest,
};

#[test]
fn semantic_fixture_keeps_labels_outside_the_workload_and_binds_records() {
    let (workload, labels, artifact, digests) = packaged::inputs().expect("semantic fixture");
    assert_eq!(workload.queries.len(), 6);
    assert_eq!(workload.candidates.len(), 18);
    assert_eq!(labels.labels.len(), workload.queries.len());
    assert_eq!(
        digests.corpus_digest,
        compute_semantic_corpus_digest(&workload, packaged::corpus_files()).expect("corpus digest")
    );
    assert!(
        workload
            .queries
            .iter()
            .any(|query| query.partition == "train")
    );
    assert!(
        workload
            .queries
            .iter()
            .any(|query| query.partition == "validation")
    );
    assert!(workload.candidates.iter().all(|candidate| {
        workload
            .corpus
            .iter()
            .any(|document| document.document_id == candidate.document_id)
    }));
    for query in &workload.queries {
        let relevant = labels
            .labels
            .iter()
            .find(|label| label.query_id == query.query_id)
            .expect("query label")
            .target_candidate_ids
            .len();
        let eligible = workload
            .candidates
            .iter()
            .filter(|candidate| query.allowed_scopes.contains(&candidate.scope))
            .count();
        assert!(eligible >= relevant + 10);
    }
    let artifact_json = serde_json::to_string(&artifact).expect("artifact JSON");
    assert!(!artifact_json.contains("rankings"));
    assert!(!artifact_json.contains("target_candidate_ids"));
}

#[test]
fn semantic_fixture_rejects_artifact_identity_and_model_manifest_mutations() {
    let (workload, labels, mut artifact, digests) = packaged::inputs().expect("semantic fixture");
    artifact.identity.model_revision.push_str("-mutated");
    let error =
        validate_semantic_ablation_inputs(&workload, &labels, &artifact, &digests.corpus_digest)
            .expect_err("artifact identity mutation must fail");
    assert!(error.to_string().contains("identity"));

    let (_workload, _labels, artifact, _digests) = packaged::inputs().expect("semantic fixture");
    let mut manifest = packaged::model_manifest_bytes().to_vec();
    manifest[0] = b'[';
    let error = validate_semantic_model_manifest(&artifact, &manifest)
        .expect_err("model manifest bytes mutation must fail");
    assert!(error.to_string().contains("manifest"));
}

#[test]
fn semantic_fixture_has_fixed_quality_and_repetition_contract() {
    let (workload, _labels, _artifact, _digests) = packaged::inputs().expect("semantic fixture");
    assert_eq!(workload.quality_contract.top_k, 10);
    assert_eq!(
        workload.quality_contract.recall_at_10_threshold_ppm,
        800_000
    );
    assert_eq!(
        workload.quality_contract.precision_at_10_threshold_ppm,
        500_000
    );
    assert!(workload.quality_contract.fixed_precision_denominator);
    assert_eq!(
        workload.repetition_contract.cold,
        SEMANTIC_ABLATION_REPETITIONS
    );
    assert_eq!(
        workload.repetition_contract.warm,
        SEMANTIC_ABLATION_REPETITIONS
    );
    assert_eq!(
        workload.repetition_contract.restart,
        SEMANTIC_ABLATION_REPETITIONS
    );
    assert!(workload.candidates.len() >= 10);
}
