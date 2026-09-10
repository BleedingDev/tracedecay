use super::{ParseOffset, RegisteredGlobalDb, TranscriptBatch};
use tracedecay_sessions::runtime::{
    SessionMessageRecord, SessionRecord, SessionStoreAccess, TranscriptGitEvidence,
    TranscriptPersistenceError,
};

pub(super) use tracedecay_sessions::runtime::store_access::{
    require_expected_offset, set_parse_offset,
};

impl RegisteredGlobalDb {
    /// Preserves session metadata while registering a validated live Start locator.
    #[hotpath::measure(
        future = true,
        label = "global_db.transcript.register_live_session_locator"
    )]
    pub async fn register_live_session_locator(
        &self,
        provider: &str,
        session_id: &str,
        project_path: &str,
        transcript_path: &str,
    ) -> Result<bool, TranscriptPersistenceError> {
        SessionStoreAccess::new(self)
            .register_live_session_locator(provider, session_id, project_path, transcript_path)
            .await
    }

    #[hotpath::measure(future = true, label = "global_db.transcript.upsert_session")]
    pub async fn upsert_session(&self, session: &SessionRecord) -> bool {
        SessionStoreAccess::new(self).upsert_session(session).await
    }

    #[hotpath::measure(future = true, label = "global_db.transcript.get_session")]
    pub async fn get_session(&self, provider: &str, session_id: &str) -> Option<SessionRecord> {
        SessionStoreAccess::new(self)
            .get_session(provider, session_id)
            .await
    }

    #[hotpath::measure(future = true, label = "global_db.transcript.get_session_result")]
    pub async fn get_session_result(
        &self,
        provider: &str,
        session_id: &str,
    ) -> Result<Option<SessionRecord>, TranscriptPersistenceError> {
        SessionStoreAccess::new(self)
            .get_session_result(provider, session_id)
            .await
    }

    #[hotpath::measure(future = true, label = "global_db.transcript.upsert_batch")]
    pub async fn upsert_transcript_batch(
        &self,
        session: &SessionRecord,
        messages: &[SessionMessageRecord],
        parse_offset_path: &str,
        parse_offset: ParseOffset,
    ) -> bool {
        SessionStoreAccess::new(self)
            .upsert_transcript_batch(session, messages, parse_offset_path, parse_offset)
            .await
    }

    #[hotpath::measure(future = true, label = "global_db.transcript.persist_batch")]
    pub async fn persist_transcript_batch_result(
        &self,
        session: &SessionRecord,
        messages: &[SessionMessageRecord],
        parse_offset_path: &str,
        expected_offset: ParseOffset,
        parse_offset: ParseOffset,
    ) -> Result<(), TranscriptPersistenceError> {
        SessionStoreAccess::new(self)
            .persist_transcript_batch_result(
                session,
                messages,
                parse_offset_path,
                expected_offset,
                parse_offset,
            )
            .await
    }

    #[hotpath::measure(
        future = true,
        label = "global_db.transcript.persist_batch_with_git_evidence"
    )]
    pub async fn persist_transcript_batch_with_git_evidence_result(
        &self,
        session: &SessionRecord,
        messages: &[SessionMessageRecord],
        parse_offset_path: &str,
        expected_offset: ParseOffset,
        parse_offset: ParseOffset,
        git_evidence: TranscriptGitEvidence<'_>,
    ) -> Result<(), TranscriptPersistenceError> {
        SessionStoreAccess::new(self)
            .persist_transcript_batch_with_git_evidence_result(
                session,
                messages,
                parse_offset_path,
                expected_offset,
                parse_offset,
                git_evidence,
            )
            .await
    }

    #[hotpath::measure(future = true, label = "global_db.transcript.persist_offset")]
    pub async fn persist_transcript_offset_result(
        &self,
        parse_offset_path: &str,
        expected_offset: ParseOffset,
        parse_offset: ParseOffset,
    ) -> Result<(), TranscriptPersistenceError> {
        SessionStoreAccess::new(self)
            .persist_transcript_offset_result(parse_offset_path, expected_offset, parse_offset)
            .await
    }

    #[hotpath::measure(
        future = true,
        label = "global_db.transcript.upsert_projection_batches"
    )]
    pub async fn upsert_transcript_projection_batches(
        &self,
        batches: &[TranscriptBatch],
        parse_offset_path: &str,
        parse_offset: ParseOffset,
    ) -> Result<(), String> {
        SessionStoreAccess::new(self)
            .upsert_transcript_projection_batches(batches, parse_offset_path, parse_offset)
            .await
    }

    #[hotpath::measure(future = true, label = "global_db.transcript.get_parse_offset")]
    pub async fn get_parse_offset(&self, path: &str) -> Option<ParseOffset> {
        SessionStoreAccess::new(self).get_parse_offset(path).await
    }

    #[hotpath::skip]
    pub async fn get_parse_offset_result(
        &self,
        path: &str,
    ) -> Result<Option<ParseOffset>, TranscriptPersistenceError> {
        SessionStoreAccess::new(self)
            .get_parse_offset_result(path)
            .await
    }

    #[hotpath::measure(future = true, label = "global_db.transcript.set_parse_offset")]
    pub async fn set_parse_offset(&self, path: &str, offset: ParseOffset) -> Result<(), String> {
        SessionStoreAccess::new(self)
            .set_parse_offset(path, offset)
            .await
    }

    #[hotpath::measure(future = true, label = "global_db.transcript.advance_parse_offset")]
    pub async fn advance_parse_offset_result(
        &self,
        path: &str,
        offset: ParseOffset,
    ) -> Result<(), TranscriptPersistenceError> {
        SessionStoreAccess::new(self)
            .advance_parse_offset_result(path, offset)
            .await
    }

    #[hotpath::measure(future = true, label = "global_db.transcript.replace_parse_offset")]
    pub async fn replace_parse_offset_result(
        &self,
        path: &str,
        expected: ParseOffset,
        next: ParseOffset,
    ) -> Result<(), TranscriptPersistenceError> {
        SessionStoreAccess::new(self)
            .replace_parse_offset_result(path, expected, next)
            .await
    }

    #[hotpath::measure(
        future = true,
        label = "global_db.transcript.replace_parse_offset_pair"
    )]
    pub async fn replace_parse_offset_pair_result(
        &self,
        first: (&str, ParseOffset, ParseOffset),
        second: (&str, ParseOffset, ParseOffset),
    ) -> Result<(), TranscriptPersistenceError> {
        SessionStoreAccess::new(self)
            .replace_parse_offset_pair_result(first, second)
            .await
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use tracedecay_domain::ProjectId;
    use tracedecay_runtime_core::db::TestDatabaseRuntimeScope;

    use super::{SessionRecord, TranscriptPersistenceError};
    use crate::tests::harness::open_registered_test_database_fixture;
    use crate::{RegisteredGlobalDbLeaseV1, RegisteredGlobalDbOwnerV1};

    async fn project_locator_store() -> (
        TempDir,
        RegisteredGlobalDbLeaseV1,
        RegisteredGlobalDbOwnerV1,
    ) {
        let directory = TempDir::new().unwrap();
        let (database, owner) = open_registered_test_database_fixture(
            &directory.path().join("sessions.db"),
            TestDatabaseRuntimeScope::ProjectSessions {
                project_id: ProjectId::new("project.live-locator").unwrap(),
            },
        )
        .await
        .unwrap();
        (directory, database, owner)
    }

    fn observed_session(transcript_path: Option<&str>) -> SessionRecord {
        SessionRecord {
            provider: "claude".to_owned(),
            session_id: "live-session".to_owned(),
            project_key: "opaque-provider-project-key".to_owned(),
            project_path: "c:/live-project".to_owned(),
            title: Some("Observed session title".to_owned()),
            started_at: Some(10),
            ended_at: Some(20),
            transcript_path: transcript_path.map(str::to_owned),
            metadata_json: Some(r#"{"model":"observed-model","custom":true}"#.to_owned()),
            parent_session_id: Some("parent-session".to_owned()),
            is_subagent: true,
            agent_id: Some("observed-agent".to_owned()),
            parent_tool_use_id: Some("parent-tool-use".to_owned()),
        }
    }

    #[tokio::test]
    async fn live_locator_insert_is_minimal_and_creates_no_history_or_transcript_file() {
        let (directory, database, _owner) = project_locator_store().await;
        let transcript = directory.path().join("native-session.jsonl");
        let transcript_path = transcript.to_str().unwrap();
        assert!(
            database
                .register_live_session_locator(
                    "claude",
                    "live-session",
                    r"\\?\C:\live-project",
                    transcript_path,
                )
                .await
                .unwrap()
        );
        assert_eq!(
            database
                .get_session_result("claude", "live-session")
                .await
                .unwrap(),
            Some(SessionRecord {
                provider: "claude".to_owned(),
                session_id: "live-session".to_owned(),
                project_key: "c:/live-project".to_owned(),
                project_path: "c:/live-project".to_owned(),
                title: None,
                started_at: None,
                ended_at: None,
                transcript_path: Some(transcript_path.to_owned()),
                metadata_json: None,
                parent_session_id: None,
                is_subagent: false,
                agent_id: None,
                parent_tool_use_id: None,
            })
        );
        let mut rows = database
            .read_connection()
            .query(
                "SELECT (SELECT COUNT(*) FROM sessions),
                        (SELECT COUNT(*) FROM session_messages),
                        (SELECT COUNT(*) FROM lcm_raw_messages),
                        (SELECT COUNT(*) FROM parse_offsets),
                        (SELECT COUNT(*) FROM observations)",
                (),
            )
            .await
            .unwrap();
        let row = rows.next().await.unwrap().unwrap();
        assert_eq!(row.get::<i64>(0).unwrap(), 1);
        for index in 1..=4 {
            assert_eq!(row.get::<i64>(index).unwrap(), 0);
        }
        assert!(!transcript.exists());
    }

    #[tokio::test]
    async fn repeated_live_locator_preserves_every_observed_session_field() {
        let (_directory, database, _owner) = project_locator_store().await;
        let expected = observed_session(Some("/native/live-session.jsonl"));
        assert!(database.upsert_session(&expected).await);

        for project_path in [r"\\?\C:\live-project", "c:/live-project"] {
            assert!(
                database
                    .register_live_session_locator(
                        "claude",
                        "live-session",
                        project_path,
                        "/native/live-session.jsonl",
                    )
                    .await
                    .unwrap()
            );
            assert_eq!(
                database
                    .get_session_result("claude", "live-session")
                    .await
                    .unwrap(),
                Some(expected.clone())
            );
        }
    }

    #[tokio::test]
    async fn conflicting_live_locator_preserves_the_existing_session() {
        let (_directory, database, _owner) = project_locator_store().await;
        let expected = observed_session(Some("/native/live-session.jsonl"));
        assert!(database.upsert_session(&expected).await);

        for (project_path, transcript_path) in [
            ("c:/other-project", "/native/live-session.jsonl"),
            ("c:/live-project", "/native/replacement.jsonl"),
            ("c:/live-project", "/native/./live-session.jsonl"),
        ] {
            assert!(
                !database
                    .register_live_session_locator(
                        "claude",
                        "live-session",
                        project_path,
                        transcript_path,
                    )
                    .await
                    .unwrap()
            );
            assert_eq!(
                database
                    .get_session_result("claude", "live-session")
                    .await
                    .unwrap(),
                Some(expected.clone())
            );
        }
    }

    #[tokio::test]
    async fn live_locator_fills_only_a_missing_locator_in_the_same_project() {
        let (_directory, database, _owner) = project_locator_store().await;
        let mut expected = observed_session(None);
        assert!(database.upsert_session(&expected).await);
        assert!(
            !database
                .register_live_session_locator(
                    "claude",
                    "live-session",
                    "c:/other-project",
                    "/native/live-session.jsonl",
                )
                .await
                .unwrap()
        );
        assert_eq!(
            database
                .get_session_result("claude", "live-session")
                .await
                .unwrap(),
            Some(expected.clone())
        );
        assert!(
            database
                .register_live_session_locator(
                    "claude",
                    "live-session",
                    "c:/live-project",
                    "/native/live-session.jsonl",
                )
                .await
                .unwrap()
        );
        expected.transcript_path = Some("/native/live-session.jsonl".to_owned());
        assert_eq!(
            database
                .get_session_result("claude", "live-session")
                .await
                .unwrap(),
            Some(expected)
        );
    }

    #[tokio::test]
    async fn concurrent_conflicting_live_locators_keep_one_winner() {
        let (_directory, database, _owner) = project_locator_store().await;
        let (first, second) = tokio::join!(
            database.register_live_session_locator(
                "codex",
                "live-session",
                "/live-project",
                "/native/first.jsonl",
            ),
            database.register_live_session_locator(
                "codex",
                "live-session",
                "/live-project",
                "/native/second.jsonl",
            ),
        );
        let first = first.unwrap();
        let second = second.unwrap();
        assert_ne!(first, second, "exactly one conflicting locator is admitted");
        let stored = database
            .get_session_result("codex", "live-session")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            stored.transcript_path.as_deref(),
            Some(if first {
                "/native/first.jsonl"
            } else {
                "/native/second.jsonl"
            })
        );
    }

    #[tokio::test]
    async fn live_locator_rejects_profile_sessions_without_inserting_a_session() {
        let directory = TempDir::new().unwrap();
        let (database, _owner) = open_registered_test_database_fixture(
            &directory.path().join("sessions.db"),
            TestDatabaseRuntimeScope::ProfileSessions,
        )
        .await
        .unwrap();
        let error = database
            .register_live_session_locator(
                "claude",
                "live-session",
                "/live-project",
                "/native/live-session.jsonl",
            )
            .await
            .unwrap_err();
        match error {
            TranscriptPersistenceError::Storage { operation, source } => {
                assert_eq!(operation, "register live session locator");
                assert_eq!(
                    source.downcast_ref::<std::io::Error>().unwrap().kind(),
                    std::io::ErrorKind::InvalidInput
                );
                assert!(source.to_string().contains("ProjectSessions"));
            }
            other => panic!("expected project-session authority refusal, got {other}"),
        }
        assert!(
            database
                .get_session_result("claude", "live-session")
                .await
                .unwrap()
                .is_none()
        );
    }
}
