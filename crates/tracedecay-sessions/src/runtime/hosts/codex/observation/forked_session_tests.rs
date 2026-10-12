use std::io::Write as _;

use serde_json::{Value, json};
use tracedecay_domain::{
    CanonicalObservationEnvelopeV1, CanonicalObservationFactV1, ObservationScopeV1, ProjectId,
};
use tracedecay_store::{ProjectionSkipReason, derive_canonical_projection};

use super::codex_observation_source_v2;
use crate::admission::HostAdmission;
use crate::admission::test_support::MemoryHostAdmission;
use crate::observation::ObservationCancellation;
use crate::runtime::hosts::codex::try_admit_codex_jsonl_observations_for_project_session_window;
use crate::runtime::jsonl_observation_admission::install_test_shared_jsonl_preparation_authority;

#[tokio::test]
async fn forked_rollout_preserves_child_ancestry_and_inherited_header_evidence() {
    install_test_shared_jsonl_preparation_authority();
    let temporary = tempfile::tempdir().unwrap();
    let home = temporary.path().canonicalize().unwrap();
    let project = home.join("project");
    let ancestor_project = home.join("ancestor-project");
    std::fs::create_dir_all(&project).unwrap();
    std::fs::create_dir_all(&ancestor_project).unwrap();
    let project_id = ProjectId::new("project.codex-forked-rollout").unwrap();
    tracedecay_runtime_core::storage::pin_fixture_repository_identity(
        &project,
        project_id.as_str(),
    )
    .unwrap();
    let child = "thread-child";
    let parent = "thread-parent";
    let ancestor = "thread-ancestor";
    let message_id = "child-message";
    let message_text = "Keep the child fork's own ancestry.";
    let records = [
        json!({
            "timestamp": "2026-10-12T12:00:00.000Z",
            "type": "session_meta",
            "payload": {
                "id": child,
                "cwd": project,
                "forked_from_id": parent,
                "thread_source": "subagent",
                "git": {"branch": "child-branch"}
            }
        }),
        json!({
            "timestamp": "2026-10-12T11:00:00.000Z",
            "type": "session_meta",
            "payload": {
                "id": parent,
                "cwd": ancestor_project,
                "forked_from_id": ancestor,
                "thread_source": "subagent",
                "model": "ancestor-model",
                "git": {"branch": "ancestor-branch"}
            }
        }),
        json!({
            "timestamp": "2026-10-12T12:00:01.000Z",
            "type": "event_msg",
            "payload": {
                "type": "item_completed",
                "thread_id": child,
                "item": {
                    "type": "UserMessage",
                    "id": message_id,
                    "content": [{"type": "text", "text": message_text}]
                }
            }
        }),
    ];
    let lines = records.map(|record| record.to_string() + "\n");
    let transcript = home.join("rollout.jsonl");
    std::fs::write(&transcript, lines.concat()).unwrap();
    let admission = MemoryHostAdmission::default();
    let cancellation = ObservationCancellation::default();
    let capture = || {
        try_admit_codex_jsonl_observations_for_project_session_window(
            &transcript,
            &project,
            project_id.clone(),
            child,
            &admission,
            u64::MAX,
            &cancellation,
        )
    };

    let progress = capture().await.unwrap();
    assert_eq!(progress.frames_persisted, 3);
    assert_eq!(progress.frames_refused, 0);
    assert!(!progress.source_deferred);
    assert!(admission.non_durable_advances().is_empty());
    let stored = admission.observations();
    assert_eq!(stored.len(), 3);
    let envelopes = stored
        .iter()
        .map(|record| {
            serde_json::from_value::<CanonicalObservationEnvelopeV1>(
                record.observation().payload().clone(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let scope = ObservationScopeV1::Project {
        project_id: project_id.clone(),
    };
    let source = codex_observation_source_v2(child).unwrap();
    let mut offset = 0;
    for ((record, envelope), line) in stored.iter().zip(&envelopes).zip(&lines) {
        let native: Value = serde_json::from_str(line).unwrap();
        let native_id = tracedecay_capture::codex::codex_native_record_id(child, &native).unwrap();
        assert_eq!(envelope.stable_record_id(), &native_id);
        assert_eq!(envelope.relations().session_id().as_str(), child);
        assert_eq!(envelope.relations().thread_id().unwrap().as_str(), child);
        assert_eq!(record.observation().scope(), &scope);
        assert_eq!(record.observation().source(), &source);
        assert_eq!(envelope.evidence().range().start(), offset);
        offset += u64::try_from(line.len()).unwrap();
        assert_eq!(envelope.evidence().range().end(), offset);
        assert!(envelope.evidence().native_timestamp().is_some());
    }
    assert_eq!(
        envelopes[0]
            .relations()
            .parent_session_id()
            .unwrap()
            .as_str(),
        parent
    );
    let inherited = &envelopes[1];
    assert!(inherited.relations().parent_session_id().is_none());
    assert!(inherited.relations().parent_agent_id().is_none());
    assert!(inherited.relations().agent_id().is_none());
    assert!(matches!(
        inherited.facts(),
        [CanonicalObservationFactV1::Session {
            project_path: Some(project_path),
            location_path: Some(location_path),
            transcript_path: Some(transcript_path),
            ..
        }] if project_path == project.to_str().unwrap()
            && location_path == project.to_str().unwrap()
            && transcript_path == transcript.to_str().unwrap()
    ));
    let child_header = derive_canonical_projection(stored[0].observation()).unwrap();
    assert_eq!(
        child_header
            .message()
            .unwrap()
            .session()
            .parent_session_id
            .as_deref(),
        Some(parent)
    );
    let inherited_header = derive_canonical_projection(stored[1].observation()).unwrap();
    assert_eq!(
        inherited_header.skip_reason(),
        Some(ProjectionSkipReason::NonConversationalRecord)
    );
    assert_eq!(inherited_header.output_count(), 0);
    let message = derive_canonical_projection(stored[2].observation()).unwrap();
    let message = message.message().unwrap();
    assert_eq!(message.session().session_id, child);
    assert_eq!(message.session().project_key, project_id.as_str());
    assert_eq!(message.session().project_path, project.to_str().unwrap());
    assert_eq!(message.message().message_id, message_id);
    assert_eq!(message.message().text, message_text);
    assert!(message.message().model.is_none());
    let cursor = admission
        .get_source_cursor(&source, &scope)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cursor.position(), offset);
    assert_eq!(offset, std::fs::metadata(&transcript).unwrap().len());

    let replay = capture().await.unwrap();
    assert_eq!(replay.frames_persisted, 0);
    assert_eq!(replay.frames_refused, 0);
    let replayed = admission.observations();
    assert_eq!(replayed.len(), stored.len());
    for (before, after) in stored.iter().zip(&replayed) {
        assert_eq!(before.observation(), after.observation());
    }
    assert_eq!(
        admission.get_source_cursor(&source, &scope).await.unwrap(),
        Some(cursor)
    );

    // A contradictory header for the child itself remains an assertion for
    // strict session reconciliation to reject, rather than becoming a no-op.
    let mut contradictory: Value = serde_json::from_str(&lines[0]).unwrap();
    contradictory["payload"]["forked_from_id"] = json!(ancestor);
    let mut writer = std::fs::OpenOptions::new()
        .append(true)
        .open(&transcript)
        .unwrap();
    writeln!(writer, "{contradictory}").unwrap();
    writer.flush().unwrap();
    let appended = capture().await.unwrap();
    assert_eq!(appended.frames_persisted, 1);
    let contradictory = admission.observations();
    let contradictory = derive_canonical_projection(contradictory[3].observation()).unwrap();
    let contradictory = contradictory.message().unwrap();
    assert_eq!(contradictory.session().session_id, child);
    assert_eq!(
        contradictory.session().parent_session_id.as_deref(),
        Some(ancestor)
    );
    assert_ne!(
        child_header.message().unwrap().session().parent_session_id,
        contradictory.session().parent_session_id
    );
}
