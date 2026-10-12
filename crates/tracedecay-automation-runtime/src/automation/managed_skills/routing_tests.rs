use tracedecay_domain::errors::TraceDecayError;

use super::{
    ManagedSkillDraft, ManagedSkillExt, ManagedSkillProvenance, ManagedSkillSource,
    ManagedSkillState, ManagedSupportFile, ManagedSupportFileExt, SkillInstallTarget,
    create_managed_skill, list_managed_skills, load_managed_skill, managed_skill_dir,
};

fn routing_draft() -> ManagedSkillDraft {
    ManagedSkillDraft {
        id: "routing".to_string(),
        title: "Routing".to_string(),
        summary: r#"Diagnose "quoted" paths"#.to_string(),
        routing_description: r#"Use when diagnosing "quoted" paths"#.to_string(),
        category: "testing".to_string(),
        targets: vec![SkillInstallTarget::Codex, SkillInstallTarget::Claude],
        body_markdown: "# Routing\n\nInspect the failing path.\n".to_string(),
        support_files: vec![
            ManagedSupportFile::new("references/paths.md", b"Preserve path identity.\n".to_vec())
                .unwrap(),
        ],
        provenance: ManagedSkillProvenance {
            source: ManagedSkillSource::AutomationRun,
            actor: "routing-test".to_string(),
            run_id: Some("run".to_string()),
        },
    }
}

#[tokio::test]
async fn released_summary_only_record_migrates_without_losing_authored_files_or_identity() {
    let profile = tempfile::TempDir::new().unwrap();
    let skill = create_managed_skill(profile.path(), routing_draft())
        .await
        .unwrap();
    create_managed_skill(
        profile.path(),
        ManagedSkillDraft {
            id: "keeper".to_string(),
            ..routing_draft()
        },
    )
    .await
    .unwrap();
    let dir = managed_skill_dir(profile.path(), &skill.metadata.id).unwrap();
    let record = dir.join("skill.json");
    let mut value = serde_json::to_value(&skill).unwrap();
    let metadata = value["metadata"].as_object_mut().unwrap();
    metadata.remove("routing_description");
    metadata.remove("activated_at");
    metadata.insert("approved_at".to_string(), serde_json::json!(12345));
    metadata.insert("state".to_string(), serde_json::json!("active"));
    metadata.insert("pinned".to_string(), serde_json::json!(true));
    metadata.insert(
        "checksum".to_string(),
        serde_json::json!("released-checksum"),
    );
    metadata.get_mut("provenance").unwrap()["source"] = serde_json::json!("user_draft");
    metadata.insert(
        "extension".to_string(),
        serde_json::json!({"retained": true}),
    );
    let bytes = serde_json::to_vec_pretty(&value).unwrap();
    std::fs::write(&record, &bytes).unwrap();
    let markdown = b"---\nid: routing\nsummary: Diagnose quoted paths\n---\n\n# Routing\n\nInspect the failing path.\n";
    std::fs::write(dir.join("SKILL.md"), markdown).unwrap();
    let support = std::fs::read(dir.join("references/paths.md")).unwrap();
    let keeper_dir = managed_skill_dir(profile.path(), "keeper").unwrap();
    let keeper_record = std::fs::read(keeper_dir.join("skill.json")).unwrap();
    let keeper_markdown = std::fs::read(keeper_dir.join("SKILL.md")).unwrap();

    let listed = list_managed_skills(profile.path()).await.unwrap();
    assert_eq!(
        listed
            .iter()
            .map(|skill| skill.metadata.id.as_str())
            .collect::<Vec<_>>(),
        ["keeper", "routing"]
    );
    let migrated = load_managed_skill(profile.path(), "routing").await.unwrap();
    assert_eq!(migrated.metadata.id, skill.metadata.id);
    assert_eq!(migrated.metadata.title, skill.metadata.title);
    assert_eq!(migrated.metadata.summary, skill.metadata.summary);
    assert_eq!(
        migrated.metadata.routing_description,
        r#"Use when Diagnose "quoted" paths"#
    );
    assert_eq!(migrated.metadata.category, skill.metadata.category);
    assert_eq!(migrated.metadata.targets, skill.metadata.targets);
    assert_eq!(migrated.metadata.state, ManagedSkillState::Active);
    assert!(migrated.metadata.pinned);
    assert_eq!(migrated.metadata.created_at, skill.metadata.created_at);
    assert_eq!(migrated.metadata.updated_at, skill.metadata.updated_at);
    assert_eq!(migrated.metadata.activated_at, Some(12345));
    assert_eq!(
        migrated.metadata.provenance.source,
        ManagedSkillSource::User
    );
    assert_eq!(
        migrated.metadata.provenance.actor,
        skill.metadata.provenance.actor
    );
    assert_eq!(
        migrated.metadata.provenance.run_id,
        skill.metadata.provenance.run_id
    );
    assert_eq!(migrated.body_markdown, skill.body_markdown);
    assert_eq!(migrated.support_files, skill.support_files);
    let mut expected = migrated.clone();
    expected.refresh_checksum();
    assert_eq!(migrated.metadata.checksum, expected.metadata.checksum);
    assert_ne!(migrated.metadata.checksum, "released-checksum");
    assert_eq!(std::fs::read(dir.join("SKILL.md")).unwrap(), markdown);
    assert_eq!(
        std::fs::read(dir.join("references/paths.md")).unwrap(),
        support
    );

    let committed = std::fs::read(&record).unwrap();
    let committed_value: serde_json::Value = serde_json::from_slice(&committed).unwrap();
    assert!(committed_value["metadata"].get("approved_at").is_none());
    assert_eq!(committed_value["metadata"]["activated_at"], 12345);
    assert_eq!(
        committed_value["metadata"]["extension"],
        serde_json::json!({"retained": true})
    );
    assert_eq!(
        load_managed_skill(profile.path(), "routing").await.unwrap(),
        migrated
    );
    list_managed_skills(profile.path()).await.unwrap();
    assert_eq!(std::fs::read(&record).unwrap(), committed);
    assert_eq!(std::fs::read(dir.join("SKILL.md")).unwrap(), markdown);
    assert_eq!(std::fs::read_dir(&dir).unwrap().count(), 3);
    assert_eq!(
        std::fs::read(keeper_dir.join("skill.json")).unwrap(),
        keeper_record
    );
    assert_eq!(
        std::fs::read(keeper_dir.join("SKILL.md")).unwrap(),
        keeper_markdown
    );
}

#[tokio::test]
async fn released_routing_preserves_the_exported_description_prefix_and_character_limit() {
    for (summary, expected) in [
        (
            "Use when inspecting paths".to_string(),
            "Use when inspecting paths".to_string(),
        ),
        (
            "uSe ThIs SkIlL WhEn inspecting paths".to_string(),
            "uSe ThIs SkIlL WhEn inspecting paths".to_string(),
        ),
        ("é".repeat(1100), format!("Use when {}", "é".repeat(1015))),
    ] {
        let profile = tempfile::TempDir::new().unwrap();
        let skill = create_managed_skill(profile.path(), routing_draft())
            .await
            .unwrap();
        let record = managed_skill_dir(profile.path(), &skill.metadata.id)
            .unwrap()
            .join("skill.json");
        let mut value = serde_json::to_value(&skill).unwrap();
        let metadata = value["metadata"].as_object_mut().unwrap();
        metadata.remove("routing_description");
        metadata.insert("summary".to_string(), serde_json::json!(summary));
        std::fs::write(&record, serde_json::to_vec_pretty(&value).unwrap()).unwrap();

        assert_eq!(
            load_managed_skill(profile.path(), "routing")
                .await
                .unwrap()
                .metadata
                .routing_description,
            expected
        );
    }
}

#[tokio::test]
async fn unresolved_released_records_are_refused_without_deleting_the_store() {
    for (pending_update, pending_file, state) in [
        (
            Some(serde_json::json!({"base_checksum": "old"})),
            false,
            "active",
        ),
        (None, true, "active"),
        (None, false, "pending_approval"),
    ] {
        let profile = tempfile::TempDir::new().unwrap();
        let skill = create_managed_skill(profile.path(), routing_draft())
            .await
            .unwrap();
        create_managed_skill(
            profile.path(),
            ManagedSkillDraft {
                id: "keeper".to_string(),
                ..routing_draft()
            },
        )
        .await
        .unwrap();
        let dir = managed_skill_dir(profile.path(), &skill.metadata.id).unwrap();
        let record = dir.join("skill.json");
        let mut value = serde_json::to_value(&skill).unwrap();
        value["metadata"]
            .as_object_mut()
            .unwrap()
            .remove("routing_description");
        value["metadata"]["state"] = serde_json::json!(state);
        if let Some(pending_update) = pending_update {
            value["pending_update"] = pending_update;
        }
        let bytes = serde_json::to_vec_pretty(&value).unwrap();
        std::fs::write(&record, &bytes).unwrap();
        if pending_file {
            std::fs::write(dir.join("pending_update.json"), b"unresolved update").unwrap();
        }
        let markdown = std::fs::read(dir.join("SKILL.md")).unwrap();
        let support = std::fs::read(dir.join("references/paths.md")).unwrap();

        assert!(load_managed_skill(profile.path(), "routing").await.is_err());
        assert!(list_managed_skills(profile.path()).await.is_err());
        assert_eq!(std::fs::read(&record).unwrap(), bytes);
        assert_eq!(std::fs::read(dir.join("SKILL.md")).unwrap(), markdown);
        assert_eq!(
            std::fs::read(dir.join("references/paths.md")).unwrap(),
            support
        );
        assert!(load_managed_skill(profile.path(), "keeper").await.is_ok());
        if pending_file {
            assert_eq!(
                std::fs::read(dir.join("pending_update.json")).unwrap(),
                b"unresolved update"
            );
        }
    }
}

#[tokio::test]
async fn conflicting_released_activation_timestamps_preserve_the_record() {
    let profile = tempfile::TempDir::new().unwrap();
    let skill = create_managed_skill(profile.path(), routing_draft())
        .await
        .unwrap();
    let dir = managed_skill_dir(profile.path(), &skill.metadata.id).unwrap();
    let record = dir.join("skill.json");
    let mut value = serde_json::to_value(&skill).unwrap();
    let metadata = value["metadata"].as_object_mut().unwrap();
    metadata.remove("routing_description");
    metadata.insert("approved_at".to_string(), serde_json::json!(100));
    metadata.insert("activated_at".to_string(), serde_json::json!(101));
    let bytes = serde_json::to_vec_pretty(&value).unwrap();
    std::fs::write(&record, &bytes).unwrap();

    let error = list_managed_skills(profile.path()).await.unwrap_err();

    assert!(
        error
            .to_string()
            .contains("approval and activation timestamps disagree")
    );
    assert_eq!(std::fs::read(&record).unwrap(), bytes);
    assert!(dir.join("SKILL.md").exists());
    assert!(dir.join("references/paths.md").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn dangling_pending_update_entry_blocks_migration_without_deletion() {
    let profile = tempfile::TempDir::new().unwrap();
    let skill = create_managed_skill(profile.path(), routing_draft())
        .await
        .unwrap();
    let dir = managed_skill_dir(profile.path(), &skill.metadata.id).unwrap();
    let record = dir.join("skill.json");
    let mut value = serde_json::to_value(&skill).unwrap();
    value["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("routing_description");
    let bytes = serde_json::to_vec_pretty(&value).unwrap();
    std::fs::write(&record, &bytes).unwrap();
    let pending_update = dir.join("pending_update.json");
    std::os::unix::fs::symlink("missing-update", &pending_update).unwrap();

    let error = list_managed_skills(profile.path()).await.unwrap_err();

    assert!(
        error
            .to_string()
            .contains("pending skill update requires resolution")
    );
    assert_eq!(std::fs::read(&record).unwrap(), bytes);
    assert!(
        std::fs::symlink_metadata(pending_update)
            .unwrap()
            .is_symlink()
    );
    assert!(dir.join("SKILL.md").exists());
}

#[cfg(unix)]
#[tokio::test]
async fn refused_metadata_publication_preserves_the_original_record_and_authored_files() {
    let profile = tempfile::TempDir::new().unwrap();
    let skill = create_managed_skill(profile.path(), routing_draft())
        .await
        .unwrap();
    let dir = managed_skill_dir(profile.path(), &skill.metadata.id).unwrap();
    let record = dir.join("skill.json");
    let mut value = serde_json::to_value(&skill).unwrap();
    value["metadata"]
        .as_object_mut()
        .unwrap()
        .remove("routing_description");
    let bytes = serde_json::to_vec_pretty(&value).unwrap();
    std::fs::write(&record, &bytes).unwrap();
    let canonical = dir.join("linked-record.json");
    std::fs::rename(&record, &canonical).unwrap();
    std::os::unix::fs::symlink("linked-record.json", &record).unwrap();
    let markdown = std::fs::read(dir.join("SKILL.md")).unwrap();
    let support = std::fs::read(dir.join("references/paths.md")).unwrap();

    assert!(list_managed_skills(profile.path()).await.is_err());

    assert_eq!(std::fs::read(&canonical).unwrap(), bytes);
    assert_eq!(std::fs::read(&record).unwrap(), bytes);
    assert!(std::fs::symlink_metadata(record).unwrap().is_symlink());
    assert_eq!(std::fs::read(dir.join("SKILL.md")).unwrap(), markdown);
    assert_eq!(
        std::fs::read(dir.join("references/paths.md")).unwrap(),
        support
    );
    assert_eq!(std::fs::read_dir(dir).unwrap().count(), 4);
}

#[tokio::test]
async fn invalid_routing_is_rejected_without_rewriting_records() {
    for (invalid, expected_error) in [
        (
            Some(serde_json::Value::Null),
            "invalid type: null, expected a string",
        ),
        (
            Some(serde_json::Value::String(String::new())),
            "native description cannot be empty",
        ),
    ] {
        let profile = tempfile::TempDir::new().unwrap();
        let skill = create_managed_skill(profile.path(), routing_draft())
            .await
            .unwrap();
        let loaded = load_managed_skill(profile.path(), &skill.metadata.id)
            .await
            .unwrap();
        assert_eq!(
            loaded.metadata.routing_description,
            r#"Use when diagnosing "quoted" paths"#
        );
        let dir = managed_skill_dir(profile.path(), &skill.metadata.id).unwrap();
        let record = dir.join("skill.json");
        let mut value = serde_json::to_value(&skill).unwrap();
        let metadata = value["metadata"].as_object_mut().unwrap();
        match invalid {
            Some(invalid) => {
                metadata.insert("routing_description".to_string(), invalid);
            }
            None => {
                metadata.remove("routing_description");
            }
        }
        let bytes = serde_json::to_vec_pretty(&value).unwrap();
        std::fs::write(&record, &bytes).unwrap();
        let markdown = std::fs::read(dir.join("SKILL.md")).unwrap();

        for error in [
            TraceDecayError::from(
                load_managed_skill(profile.path(), &skill.metadata.id)
                    .await
                    .unwrap_err(),
            ),
            list_managed_skills(profile.path()).await.unwrap_err(),
        ] {
            assert!(
                matches!(&error, TraceDecayError::Config { message } if message.contains(expected_error)),
                "unexpected rejection: {error:?}"
            );
        }
        assert_eq!(std::fs::read(&record).unwrap(), bytes);
        assert_eq!(std::fs::read(dir.join("SKILL.md")).unwrap(), markdown);
    }
}
