    use super::*;
    use crate::{
        SemanticEvaluationCancellationV1, SemanticExecutionAuthority, SemanticExecutionInterruptionV1,
    };
    use std::collections::BTreeMap;
    #[cfg(all(feature = "semantic-fastembed", not(windows)))]
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::sync::Barrier;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::sync::mpsc::{self, Receiver, SyncSender};
    use std::time::Duration;
    #[cfg(all(feature = "semantic-fastembed", not(windows)))]
    use std::time::Instant;

    use tracedecay_domain::{
        EmbeddingDeviceClassV1 as DeviceClassV1, EmbeddingMetricV1 as SemanticMetricV1,
        EmbeddingNormalizationV1, EmbeddingPoolingV1, EmbeddingPrecisionV1,
        EmbeddingTruncationSideV1 as TruncationSideV1,
    };

    /// Ceilings with the resident bound pinned to the shipped default.
    ///
    /// Composition resolves an unpinned ceiling against the host before any
    /// artifact is built; these lifecycle tests are about installation and
    /// verification, so they pin the value rather than model the host.
    fn pinned_ceilings() -> SemanticResourceCeilings {
        SemanticResourceCeilings {
            max_resident_bytes: Some(tracedecay_semantic_contracts::DEFAULT_SEMANTIC_RESIDENT_BYTES),
            ..SemanticResourceCeilings::default()
        }
    }
    use super::super::model_catalog::{
        CatalogMemberPinV1, CatalogSourceV1, CatalogedEmbeddingBackendV1,
    };
    use tracedecay_semantic_contracts::{
        ArtifactMemberPinV1, ArtifactMemberRoleV1, ArtifactPackageMemberV1, ArtifactProfileKindV1,
        DEFAULT_FASTEMBED_MODEL_ID, MODEL_ARTIFACT_MANIFEST_SCHEMA_V1, ModelArtifactManifestPayloadV1,
        ModelArtifactManifestV1, PlatformTargetV1, RerankCompatibilityPinsV1,
        RerankerArtifactLifecycleStatusV1, ResourceCeilingV1, RuntimeCompatibilityV1,
        SemanticModelLifecycleStateV1, SemanticResourceCeilings, Sha256DigestHex, TruncationPolicyV1,
        UpstreamSourceV1,
    };

    struct FixtureSource {
        root: PathBuf,
        calls: AtomicUsize,
    }
    impl ModelMemberSourceV1 for FixtureSource {
        fn fetch_member(
            &self,
            _model: &CatalogedFastEmbedModelV1,
            upstream_path: &str,
            destination: &Path,
        ) -> Result<(), ModelLifecycleErrorV1> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let source = self.root.join(upstream_path);
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent).map_err(|_| ModelLifecycleErrorV1::DownloadFailed)?;
            }
            fs::copy(&source, destination).map_err(|_| ModelLifecycleErrorV1::DownloadFailed)?;
            Ok(())
        }
}
    struct BlockingFixtureSource {
        root: PathBuf,
        calls: AtomicUsize,
        entered: SyncSender<()>,
        release: Mutex<Receiver<()>>,
    }

    impl ModelMemberSourceV1 for BlockingFixtureSource {
        fn fetch_member(
            &self,
            _model: &CatalogedFastEmbedModelV1,
            upstream_path: &str,
            destination: &Path,
        ) -> Result<(), ModelLifecycleErrorV1> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.entered
                    .send(())
                    .map_err(|_| ModelLifecycleErrorV1::DownloadFailed)?;
                self.release
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .recv()
                    .map_err(|_| ModelLifecycleErrorV1::DownloadFailed)?;
            }
            let source = self.root.join(upstream_path);
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent).map_err(|_| ModelLifecycleErrorV1::DownloadFailed)?;
            }
            fs::copy(source, destination)
                .map(|_| ())
                .map_err(|_| ModelLifecycleErrorV1::DownloadFailed)
        }
    }

    struct PanickingFixtureSource;

    impl ModelMemberSourceV1 for PanickingFixtureSource {
        fn fetch_member(
            &self,
            _model: &CatalogedFastEmbedModelV1,
            _upstream_path: &str,
            _destination: &Path,
        ) -> Result<(), ModelLifecycleErrorV1> {
            panic!("fixture acquisition worker panic")
        }
    }

    struct FailOncePanickingFixtureSource {
        root: PathBuf,
        calls: AtomicUsize,
    }

    impl ModelMemberSourceV1 for FailOncePanickingFixtureSource {
        fn fetch_member(
            &self,
            _model: &CatalogedFastEmbedModelV1,
            upstream_path: &str,
            destination: &Path,
        ) -> Result<(), ModelLifecycleErrorV1> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("fail-once fixture acquisition worker")
            }
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent).map_err(|_| ModelLifecycleErrorV1::DownloadFailed)?;
            }
            fs::copy(self.root.join(upstream_path), destination)
                .map(|_| ())
                .map_err(|_| ModelLifecycleErrorV1::DownloadFailed)
        }
    }

    struct BlockingPanickingFixtureSource {
        entered: SyncSender<()>,
        release: Mutex<Receiver<()>>,
    }

    impl ModelMemberSourceV1 for BlockingPanickingFixtureSource {
        fn fetch_member(
            &self,
            _model: &CatalogedFastEmbedModelV1,
            _upstream_path: &str,
            _destination: &Path,
        ) -> Result<(), ModelLifecycleErrorV1> {
            self.entered
                .send(())
                .expect("blocking panic entered receiver");
            self.release
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .recv()
                .expect("blocking panic release sender");
            panic!("fixture acquisition worker panic after release")
        }
    }

    struct BlockingFailOncePanickingFixtureSource {
        root: PathBuf,
        calls: AtomicUsize,
        entered: SyncSender<()>,
        release: Mutex<Receiver<()>>,
    }

    impl ModelMemberSourceV1 for BlockingFailOncePanickingFixtureSource {
        fn fetch_member(
            &self,
            _model: &CatalogedFastEmbedModelV1,
            upstream_path: &str,
            destination: &Path,
        ) -> Result<(), ModelLifecycleErrorV1> {
            if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
                self.entered
                    .send(())
                    .map_err(|_| ModelLifecycleErrorV1::DownloadFailed)?;
                self.release
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .recv()
                    .map_err(|_| ModelLifecycleErrorV1::DownloadFailed)?;
                panic!("fail-once fixture acquisition worker")
            }
            if let Some(parent) = destination.parent() {
                fs::create_dir_all(parent).map_err(|_| ModelLifecycleErrorV1::DownloadFailed)?;
            }
            fs::copy(self.root.join(upstream_path), destination)
                .map(|_| ())
                .map_err(|_| ModelLifecycleErrorV1::DownloadFailed)
        }
    }

    /// A local stand-in for the model hub serving one fixture model.
    ///
    /// The listener is bound and listening before the endpoint is handed out,
    /// and it stays in blocking mode: a nonblocking listener hands out
    /// nonblocking accepted sockets on Windows, so the request reader would
    /// fail with `WouldBlock` (WSAEWOULDBLOCK 10035) instead of waiting for
    /// bytes. The worker accepts until it has served every expected request;
    /// `finish` stops it early through a wake-up connection if the client
    /// never issued them, so a short acquisition fails an assertion instead
    /// of hanging the join.
    #[cfg(all(feature = "semantic-fastembed", not(windows)))]
    struct FixtureHub {
        endpoint: String,
        address: SocketAddr,
        requests: Arc<AtomicUsize>,
        stop_requested: Arc<AtomicBool>,
        worker: Option<JoinHandle<()>>,
    }

    #[cfg(all(feature = "semantic-fastembed", not(windows)))]
    impl FixtureHub {
        fn start(model: &CatalogedFastEmbedModelV1, fixture: &Path) -> Self {
            let members = model
                .members
                .values()
                .map(|member| {
                    (
                        member.upstream_path.clone(),
                        fs::read(fixture.join(&member.path)).unwrap(),
                    )
                })
                .collect::<BTreeMap<_, _>>();
            let revision = model.source.revision.clone();
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let endpoint = format!("http://{address}");
            let requests = Arc::new(AtomicUsize::new(0));
            let request_counter = Arc::clone(&requests);
            let stop_requested = Arc::new(AtomicBool::new(false));
            let stop_observed = Arc::clone(&stop_requested);
            let worker = thread::spawn(move || {
                let expected_requests = members.len() * 2;
                while request_counter.load(Ordering::SeqCst) < expected_requests {
                    let (mut stream, _) = listener
                        .accept()
                        .unwrap_or_else(|error| panic!("fixture hub accept failed: {error}"));
                    if stop_observed.load(Ordering::SeqCst) {
                        return;
                    }
                    serve_fixture_hub_request(&mut stream, &members, &revision, &request_counter);
                }
            });
            Self {
                endpoint,
                address,
                requests,
                stop_requested,
                worker: Some(worker),
            }
        }

        fn finish(mut self) -> usize {
            let worker = self.worker.take().unwrap();
            self.stop_requested.store(true, Ordering::SeqCst);
            if !worker.is_finished() {
                let _ = TcpStream::connect(self.address);
            }
            worker.join().unwrap();
            self.requests.load(Ordering::SeqCst)
        }
    }

    #[cfg(all(feature = "semantic-fastembed", not(windows)))]
    fn serve_fixture_hub_request(
        stream: &mut TcpStream,
        members: &BTreeMap<String, Vec<u8>>,
        revision: &str,
        requests: &AtomicUsize,
    ) {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut request = Vec::with_capacity(1024);
        loop {
            let mut chunk = [0_u8; 1024];
            let read = stream.read(&mut chunk).unwrap();
            if read == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        let request = String::from_utf8_lossy(&request);
        let request_line = request.lines().next().unwrap();
        let path = request_line.split_whitespace().nth(1).unwrap();
        let resolve_marker = format!("/resolve/{revision}/");
        let upstream_path = path.split_once(&resolve_marker).map_or_else(
            || panic!("unexpected fixture hub request path: {path}"),
            |(_, upstream_path)| upstream_path,
        );
        let body = members
            .get(upstream_path)
            .unwrap_or_else(|| panic!("unexpected fixture hub request path: {path}"));
        let metadata_request = request.to_ascii_lowercase().contains("range: bytes=0-0");
        let response_body = if metadata_request { &body[..1] } else { body };
        let end = if metadata_request { 0 } else { body.len() - 1 };
        let etag = hex::encode(Sha256::digest(body));
        write!(
            stream,
            "HTTP/1.1 206 Partial Content\r\n\
             Content-Length: {}\r\n\
             Content-Range: bytes 0-{end}/{}\r\n\
             ETag: \"{etag}\"\r\n\
             X-Repo-Commit: {revision}\r\n\
             Connection: close\r\n\r\n",
            response_body.len(),
            body.len(),
        )
        .unwrap();
        stream.write_all(response_body).unwrap();
        stream.flush().unwrap();
        requests.fetch_add(1, Ordering::SeqCst);
    }

    fn join_background_acquisition(
        owner: &SemanticModelLifecycleOwnerV1,
    ) -> Result<(), ModelLifecycleErrorV1> {
        owner
            .worker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .handle
            .take()
            .expect("background acquisition worker")
            .join()
            .expect("background acquisition worker must not panic")
    }

    fn tiny_catalog(fixture: &Path) -> (FastEmbedModelCatalogV1, String) {
        let members_dir = fixture;
        fs::create_dir_all(members_dir).unwrap();
        let mut members = BTreeMap::new();
        for (role, name, bytes) in [
            ("model", "model.onnx", b"onnx-bytes".as_slice()),
            ("tokenizer", "tokenizer.json", br#"{"ok":true}"#.as_slice()),
            ("config", "config.json", br#"{"dim":8}"#.as_slice()),
            (
                "special_tokens_map",
                "special_tokens_map.json",
                br"{}".as_slice(),
            ),
            (
                "tokenizer_config",
                "tokenizer_config.json",
                br"{}".as_slice(),
            ),
        ] {
            let path = members_dir.join(name);
            fs::write(&path, bytes).unwrap();
            members.insert(
                role.to_owned(),
                CatalogMemberPinV1 {
                    path: name.to_owned(),
                    upstream_path: name.to_owned(),
                    length: bytes.len() as u64,
                    sha256: hex::encode(Sha256::digest(bytes)),
                },
            );
        }
        let model = CatalogedFastEmbedModelV1 {
            model_id: "TinyFixtureModel".to_owned(),
            backend: CatalogedEmbeddingBackendV1::FastEmbedOrt {
                fastembed_enum: "TinyFixtureModel".to_owned(),
            },
            model_code: "tracedecay/tiny-fixture".to_owned(),
            source: CatalogSourceV1 {
                upstream: "https://example.invalid/tracedecay/tiny-fixture".to_owned(),
                revision: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_owned(),
                license: "Apache-2.0".to_owned(),
                license_url: "https://www.apache.org/licenses/LICENSE-2.0".to_owned(),
                provenance: "https://example.invalid/tracedecay/tiny-fixture/tree/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
            },
            expected_dimensions: 8,
            max_length: 32,
            members,
        };
        // Production validate requires default Jina; for unit tests build a
        // catalog that includes both the default pin and the tiny fixture.
        let mut catalog = FastEmbedModelCatalogV1::production();
        catalog.models.push(model.clone());
        (catalog, model.model_id)
    }

    fn tiny_manifest(model: &CatalogedFastEmbedModelV1) -> ModelArtifactManifestV1 {
        let role = |name: &str| match name {
            "model" => ArtifactMemberRoleV1::Model,
            "tokenizer" => ArtifactMemberRoleV1::Tokenizer,
            "config" => ArtifactMemberRoleV1::Config,
            "special_tokens_map" => ArtifactMemberRoleV1::SpecialTokensMap,
            "tokenizer_config" => ArtifactMemberRoleV1::TokenizerConfig,
            _ => unreachable!(),
        };
        let members: Vec<_> = model
            .members
            .iter()
            .map(|(name, pin)| ArtifactPackageMemberV1 {
                role: role(name),
                path: pin.path.clone(),
                digest: Sha256DigestHex::new(pin.sha256.clone()).unwrap(),
                byte_length: pin.length,
            })
            .collect();
        let member = |role| members.iter().find(|member| member.role == role).unwrap();
        let model_member = member(ArtifactMemberRoleV1::Model);
        ModelArtifactManifestV1 {
            payload: ModelArtifactManifestPayloadV1 {
                schema: MODEL_ARTIFACT_MANIFEST_SCHEMA_V1.to_owned(),
                artifact_id: model.model_id.clone(),
                profile_kind: ArtifactProfileKindV1::Embedding,
                spdx_license: model.source.license.clone(),
                model_member: ArtifactMemberPinV1 {
                    digest: model_member.digest.clone(),
                    byte_length: model_member.byte_length,
                },
                tokenizer_digest: member(ArtifactMemberRoleV1::Tokenizer).digest.clone(),
                config_digest: member(ArtifactMemberRoleV1::Config).digest.clone(),
                query_instruction_digest: None,
                document_instruction_digest: None,
                members,
                dimensions: model.expected_dimensions,
                metric: SemanticMetricV1::Cosine,
                normalization: EmbeddingNormalizationV1::L2,
                pooling: EmbeddingPoolingV1::Mean,
                truncation: TruncationPolicyV1 {
                    side: TruncationSideV1::Right,
                    max_length: model.max_length,
                },
                precision: EmbeddingPrecisionV1::Fp32,
                runtime: RuntimeCompatibilityV1 {
                    runtime: super::super::artifact_store::FASTEMBED_RUNTIME_FAMILY_V1.to_owned(),
                    build_revision: super::super::artifact_store::FASTEMBED_RUNTIME_BUILD_REVISION_V1
                        .to_owned(),
                    platforms: vec![PlatformTargetV1 {
                        os: std::env::consts::OS.to_owned(),
                        arch: std::env::consts::ARCH.to_owned(),
                    }],
                },
                device: DeviceClassV1::Cpu,
                resource_ceiling: ResourceCeilingV1 {
                    max_model_bytes: 1_024,
                    max_tokenizer_bytes: 1_024,
                    max_resident_bytes: 4_096,
                    max_threads: 1,
                    max_batch_size: 1,
                    max_sequence_length: model.max_length,
                    load_deadline_ms: 1_000,
                },
                upstream: UpstreamSourceV1 {
                    name: model.model_code.clone(),
                    version: "fixture".to_owned(),
                    revision: model.source.revision.clone(),
                },
            },
        }
    }

    fn scoped_hub_source(root: &Path) -> Arc<dyn ModelMemberSourceV1> {
        Arc::new(HfHubModelMemberSourceV1::new(
            root.join(HF_HUB_CACHE_DIRECTORY_V1),
        ))
    }

    #[test]
    fn default_selection_is_selected_not_downloaded_and_offline_safe() {
        let root = tempfile::tempdir().unwrap();
        let owner = SemanticModelLifecycleOwnerV1::open_default(root.path()).unwrap();
        let status = owner.status();
        assert_eq!(
            status.selected_model.as_deref(),
            Some(DEFAULT_FASTEMBED_MODEL_ID)
        );
        assert!(!status.auto_download);
        assert!(status.semantics_omitted);
        assert!(matches!(
            status.state,
            Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded { .. })
        ));
        assert!(status.remediation.retry);
        assert!(!owner.enqueue_demand_acquisition_if_needed());
    }

    #[cfg(all(feature = "semantic-fastembed", not(windows)))]
    #[test]
    fn fresh_hub_acquisition_downloads_then_reuses_private_cache_offline() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let online_root = tempfile::tempdir().unwrap();
        let hub = FixtureHub::start(&model, fixture.path());
        let cache = online_root.path().join(HF_HUB_CACHE_DIRECTORY_V1);
        let online_source = Arc::new(HfHubModelMemberSourceV1::new_for_tests(
            cache.clone(),
            Some(hub.endpoint.clone()),
            false,
        ));
        let online =
            SemanticModelLifecycleOwnerV1::open(online_root.path(), catalog.clone(), online_source)
                .unwrap();

        online.select_model(Some(&model_id), true).unwrap();
        assert!(online.enqueue_demand_acquisition_if_needed());
        join_background_acquisition(&online).expect("online acquisition must complete");
        assert!(matches!(
            online.status().state,
            Some(SemanticModelLifecycleStateV1::Installed { .. })
        ));
        assert_eq!(hub.finish(), model.members.len() * 2);

        let offline_root = tempfile::tempdir().unwrap();
        let offline_source = Arc::new(HfHubModelMemberSourceV1::new_for_tests(cache, None, true));
        let offline =
            SemanticModelLifecycleOwnerV1::open(offline_root.path(), catalog, offline_source).unwrap();
        offline.select_model(Some(&model_id), true).unwrap();
        offline.acquire_blocking_for_tests().unwrap();

        assert!(matches!(
            offline.status().state,
            Some(SemanticModelLifecycleStateV1::Installed { .. })
        ));
    }

    #[cfg(all(feature = "semantic-fastembed", not(windows)))]
    #[test]
    fn offline_cache_miss_reports_failed_reason_and_omits_semantics() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let root = tempfile::tempdir().unwrap();
        let source = Arc::new(HfHubModelMemberSourceV1::new_for_tests(
            root.path().join(HF_HUB_CACHE_DIRECTORY_V1),
            None,
            true,
        ));
        let owner = SemanticModelLifecycleOwnerV1::open(root.path(), catalog, source).unwrap();

        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        assert!(matches!(
            join_background_acquisition(&owner),
            Err(ModelLifecycleErrorV1::DownloadFailed
                | ModelLifecycleErrorV1::DownloadFailedWithReason(_))
        ));

        let status = owner.status();
        let Some(SemanticModelLifecycleStateV1::Failed {
            detail, retryable, ..
        }) = status.state
        else {
            panic!("offline cache miss must report failed acquisition: {status:?}");
        };
        assert!(retryable);
        assert!(detail.contains("offline"));
        assert!(detail.contains("config.json"));
        assert!(status.semantics_omitted);
    }

    #[test]
    fn store_failure_does_not_leave_acquisition_stuck_in_progress() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let root = tempfile::tempdir().unwrap();
        let source = Arc::new(FixtureSource {
            root: fixture.path().to_path_buf(),
            calls: AtomicUsize::new(0),
        });
        let owner = SemanticModelLifecycleOwnerV1::open(root.path(), catalog, source).unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        fs::remove_dir(root.path().join("staging")).unwrap();
        fs::write(root.path().join("staging"), b"not a directory").unwrap();

        assert_eq!(
            owner.acquire_blocking_for_tests().unwrap_err(),
            ModelLifecycleErrorV1::StoreUnavailable
        );
        let status = owner.status();
        let Some(SemanticModelLifecycleStateV1::Failed {
            detail, retryable, ..
        }) = status.state
        else {
            panic!("store failure must terminate acquisition state: {status:?}");
        };
        assert!(retryable);
        assert_eq!(detail, ModelLifecycleErrorV1::StoreUnavailable.to_string());
        assert!(status.semantics_omitted);
    }

    #[cfg(unix)]
    #[test]
    fn open_rejects_symlinked_private_lifecycle_bases() {
        use std::os::unix::fs::symlink;

        let fixture = tempfile::tempdir().unwrap();
        let (catalog, _model_id) = tiny_catalog(fixture.path());
        for base in ["staging", "installs", "quarantine"] {
            let root = tempfile::tempdir().unwrap();
            let outside = tempfile::tempdir().unwrap();
            symlink(outside.path(), root.path().join(base)).unwrap();

            let result = SemanticModelLifecycleOwnerV1::open(
                root.path(),
                catalog.clone(),
                Arc::new(FixtureSource {
                    root: fixture.path().to_path_buf(),
                    calls: AtomicUsize::new(0),
                }),
            );

            assert!(matches!(
                result,
                Err(ModelLifecycleErrorV1::StoreUnavailable)
            ));
            assert!(outside.path().read_dir().unwrap().next().is_none());
        }
    }

    #[cfg(unix)]
    #[test]
    fn private_cleanup_rejects_a_symlinked_lifecycle_base() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        fs::create_dir_all(root.path().join("staging")).unwrap();
        fs::remove_dir(root.path().join("staging")).unwrap();
        symlink(outside.path(), root.path().join("staging")).unwrap();
        let candidate = root
            .path()
            .join("staging")
            .join(".previous-install-TinyFixtureModel-0123456789abcdef-1-2-3-4");
        fs::create_dir_all(outside.path().join(candidate.file_name().unwrap())).unwrap();
        let sentinel = outside
            .path()
            .join(candidate.file_name().unwrap())
            .join("sentinel");
        fs::write(&sentinel, b"retain me").unwrap();

        assert!(!private_cleanup_path_allowed(root.path(), &candidate));
        assert_eq!(
            cleanup_private_owned_path(root.path(), &candidate),
            Err(ModelLifecycleErrorV1::Rejected)
        );
        assert_eq!(fs::read(sentinel).unwrap(), b"retain me");
    }

    #[cfg(unix)]
    #[test]
    fn acquisition_claim_treats_dangling_symlink_and_race_as_collisions() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        ensure_lifecycle_private_directories(root.path()).unwrap();
        let model_id = "TinyFixtureModel";
        let digest = "0123456789abcdef0123456789abcdef";
        let base = staging_path_for(root.path(), model_id, digest);
        symlink(
            root.path().join("missing-staging-target"),
            &base,
        )
        .unwrap();

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let first_root = root.path().to_path_buf();
        let first_barrier = Arc::clone(&barrier);
        let first = thread::spawn(move || {
            first_barrier.wait();
            claim_staging_path_for_acquisition(&first_root, model_id, digest).unwrap()
        });
        let second_root = root.path().to_path_buf();
        let second_barrier = Arc::clone(&barrier);
        let second = thread::spawn(move || {
            second_barrier.wait();
            claim_staging_path_for_acquisition(&second_root, model_id, digest).unwrap()
        });
        let first = first.join().unwrap();
        let second = second.join().unwrap();

        assert_ne!(first, base, "dangling symlink must occupy the deterministic base");
        assert_ne!(second, base);
        assert_ne!(first, second, "concurrent claims must be distinct directories");
        assert!(fs::symlink_metadata(&base).unwrap().file_type().is_symlink());
        assert!(first.is_dir());
        assert!(second.is_dir());
        cleanup_private_owned_path(root.path(), &first).unwrap();
        cleanup_private_owned_path(root.path(), &second).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn private_install_symlink_escape_is_rejected_for_runtime_and_rollback_admission() {
        use std::os::unix::fs::symlink;

        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let digest = catalog_package_digest(&model);
        let root = tempfile::tempdir().unwrap();
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog.clone(),
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        let outside = tempfile::tempdir().unwrap();
        let sentinel = outside.path().join("sentinel");
        fs::write(&sentinel, b"retain me").unwrap();
        let install = install_path_for(root.path(), &model_id, &model.source.revision, &digest);
        fs::create_dir_all(install.parent().unwrap()).unwrap();
        symlink(outside.path(), &install).unwrap();

        {
            let mut guard = owner.inner.writer();
            guard.durable.selected_model = Some(model_id.clone());
            guard.durable.state = Some(SemanticModelLifecycleStateV1::Installed {
                model_id: model_id.clone(),
                revision: model.source.revision.clone(),
                artifact_digest: digest.clone(),
                install_path: install.clone(),
            });
            persist_durable(root.path(), &guard.durable).unwrap();
        }
        assert_eq!(owner.lifecycle_mutation_target(), None);
        assert_eq!(
            owner.select_model(Some(&model_id), true),
            Err(ModelLifecycleErrorV1::VerificationFailed)
        );

        {
            let mut guard = owner.inner.writer();
            guard.durable.state = Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded {
                model_id: model_id.clone(),
                revision: model.source.revision.clone(),
                artifact_digest: digest.clone(),
            });
            guard.durable.previous_ready = Some(SemanticModelLifecycleStateV1::Installed {
                model_id: model_id.clone(),
                revision: model.source.revision.clone(),
                artifact_digest: digest,
                install_path: install,
            });
            persist_durable(root.path(), &guard.durable).unwrap();
        }
        assert_eq!(
            owner.rollback_to_previous(),
            Err(ModelLifecycleErrorV1::Rejected)
        );
        drop(owner);
        let reopened = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        assert!(matches!(
            reopened.status().state,
            Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded { .. })
        ));
        assert!(!reopened.status().remediation.rollback);
        assert_eq!(
            reopened.rollback_to_previous(),
            Err(ModelLifecycleErrorV1::Rejected)
        );
        assert_eq!(fs::read(sentinel).unwrap(), b"retain me");
    }

    #[test]
    fn lifecycle_targets_reject_non_owned_current_and_previous_paths() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let digest = catalog_package_digest(&model);
        let root = tempfile::tempdir().unwrap();
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        let outside = tempfile::tempdir().unwrap();
        let invalid_paths = [
            root.path()
                .join("staging")
                .join(format!("{model_id}-{digest}")),
            root.path()
                .join("quarantine")
                .join(format!("acquisition-1-2-3-4-{model_id}-{digest}")),
            outside.path().join("external-install"),
        ];

        for invalid_path in invalid_paths {
            if let Some(parent) = invalid_path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::create_dir_all(&invalid_path).unwrap();
            let invalid_state = SemanticModelLifecycleStateV1::Installed {
                model_id: model_id.clone(),
                revision: model.source.revision.clone(),
                artifact_digest: digest.clone(),
                install_path: invalid_path.clone(),
            };
            {
                let mut guard = owner.inner.writer();
                guard.durable.selected_model = Some(model_id.clone());
                guard.durable.state = Some(invalid_state.clone());
                guard.durable.previous_ready = None;
                persist_durable(root.path(), &guard.durable).unwrap();
            }
            assert_eq!(
                owner.lifecycle_mutation_target(),
                None,
                "current target must reject {invalid_path:?}"
            );

            {
                let mut guard = owner.inner.writer();
                guard.durable.state = Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded {
                    model_id: model_id.clone(),
                    revision: model.source.revision.clone(),
                    artifact_digest: digest.clone(),
                });
                guard.durable.previous_ready = Some(invalid_state);
                persist_durable(root.path(), &guard.durable).unwrap();
            }
            assert_eq!(
                owner.rollback_to_previous(),
                Err(ModelLifecycleErrorV1::Rejected),
                "rollback must reject {invalid_path:?}"
            );
            fs::remove_dir_all(&invalid_path).unwrap();
        }
    }

    #[test]
    fn background_cleanup_uses_the_collision_selected_staging_path() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let digest = catalog_package_digest(&model);
        let root = tempfile::tempdir().unwrap();
        let deterministic_base = staging_path_for(root.path(), &model_id, &digest);
        fs::create_dir_all(&deterministic_base).unwrap();
        let orphan = deterministic_base.join("orphan-from-previous-run");
        fs::write(&orphan, b"retain me").unwrap();
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let owner = Arc::new(
            SemanticModelLifecycleOwnerV1::open(
                root.path(),
                catalog,
                Arc::new(BlockingFixtureSource {
                    root: fixture.path().to_path_buf(),
                    calls: AtomicUsize::new(0),
                    entered: entered_tx,
                    release: Mutex::new(release_rx),
                }),
            )
            .unwrap(),
        );
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("background acquisition entered fixture source");

        let actual_staging = owner
            .worker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .active_token
            .as_ref()
            .expect("active worker token")
            .staging_path
            .clone();
        assert_ne!(actual_staging, deterministic_base);
        assert!(actual_staging.is_dir());

        let joining_owner = Arc::clone(&owner);
        let (joined_tx, joined_rx) = mpsc::sync_channel(1);
        let joiner = thread::spawn(move || {
            joined_tx
                .send(joining_owner.cancel_and_join_background_acquisition_until(
                    std::time::Instant::now() + Duration::from_secs(1),
                ))
                .unwrap();
        });
        assert!(
            joined_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "join must wait for the blocked source"
        );
        release_tx.send(()).unwrap();
        assert_eq!(
            joined_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("joined cancellation outcome"),
            Ok(true)
        );
        joiner.join().unwrap();
        assert!(!actual_staging.exists());
        assert_eq!(fs::read(orphan).unwrap(), b"retain me");
        assert!(deterministic_base.is_dir());
    }

    #[test]
    fn quarantine_names_refuse_restart_collisions_without_replacing_debt() {
        let root = tempfile::tempdir().unwrap();
        ensure_lifecycle_private_directories(root.path()).unwrap();
        let leaf = "TinyFixtureModel-0123456789abcdef";
        let first = quarantine_path_for_seed(root.path(), 7, leaf, 11, 13).unwrap();
        fs::write(&first, b"old cleanup debt").unwrap();
        let second = quarantine_path_for_seed(root.path(), 7, leaf, 11, 13).unwrap();

        assert_ne!(first, second);
        assert!(private_cleanup_path_allowed(root.path(), &first));
        assert!(private_cleanup_path_allowed(root.path(), &second));
        assert_eq!(fs::read(&first).unwrap(), b"old cleanup debt");
        fs::write(&second, b"new cleanup debt").unwrap();
        assert_eq!(fs::read(&first).unwrap(), b"old cleanup debt");
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn quarantine_move_refuses_a_destination_occupied_at_the_move_boundary() {
        let root = tempfile::tempdir().unwrap();
        ensure_lifecycle_private_directories(root.path()).unwrap();
        let source = staging_path_for(root.path(), "TinyFixtureModel", "0123456789abcdef");
        let destination = quarantine_path_for_seed(
            root.path(),
            7,
            "TinyFixtureModel-0123456789abcdef",
            11,
            13,
        )
        .unwrap();
        fs::write(&source, b"source cleanup bytes").unwrap();
        fs::write(&destination, b"existing cleanup debt").unwrap();

        let error = rename_quarantine_noreplace(&source, &destination).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        assert_eq!(fs::read(&source).unwrap(), b"source cleanup bytes");
        assert_eq!(fs::read(&destination).unwrap(), b"existing cleanup debt");
    }

    #[cfg(unix)]
    #[test]
    fn private_backup_names_treat_dangling_symlinks_as_collisions() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        ensure_lifecycle_private_directories(root.path()).unwrap();
        let model_id = "TinyFixtureModel";
        let digest = "0123456789abcdef0123456789abcdef";
        let first = private_backup_path_for_seed(root.path(), model_id, digest, 7, 11, 13);
        symlink(root.path().join("missing-backup-target"), &first).unwrap();
        let second = private_backup_path_for_seed(root.path(), model_id, digest, 7, 11, 13);

        assert_ne!(first, second);
        assert!(fs::symlink_metadata(&first).unwrap().file_type().is_symlink());
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn private_backup_move_refuses_a_dangling_marker_at_the_move_boundary() {
        use std::os::unix::fs::symlink;

        let root = tempfile::tempdir().unwrap();
        ensure_lifecycle_private_directories(root.path()).unwrap();
        let source = staging_path_for(root.path(), "TinyFixtureModel", "0123456789abcdef");
        let destination =
            private_backup_path_for_seed(root.path(), "TinyFixtureModel", "0123456789abcdef", 7, 11, 13);
        fs::create_dir_all(&source).unwrap();
        fs::write(source.join("sentinel"), b"source backup bytes").unwrap();
        symlink(root.path().join("missing-backup-target"), &destination).unwrap();

        let error = rename_quarantine_noreplace(&source, &destination).unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::AlreadyExists);
        assert!(source.is_dir());
        assert!(fs::symlink_metadata(&destination).unwrap().file_type().is_symlink());
    }

    #[test]
    fn background_backup_cleanup_failure_preserves_the_published_install() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let digest = catalog_package_digest(&model);
        let root = tempfile::tempdir().unwrap();
        let install = install_path_for(root.path(), &model_id, &model.source.revision, &digest);
        let owner = Arc::new(
            SemanticModelLifecycleOwnerV1::open(
                root.path(),
                catalog.clone(),
                Arc::new(FixtureSource {
                    root: fixture.path().to_path_buf(),
                    calls: AtomicUsize::new(0),
                }),
            )
            .unwrap(),
        );
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            let installed = matches!(
                owner.status().state,
                Some(SemanticModelLifecycleStateV1::Installed { .. })
            );
            let finished = owner
                .worker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .handle
                .as_ref()
                .is_some_and(JoinHandle::is_finished);
            if installed && finished {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "background acquisition did not finish its initial install"
            );
            thread::yield_now();
        }
        assert_eq!(
            owner.cancel_and_join_background_acquisition_until(
                std::time::Instant::now() + Duration::from_secs(1),
            ),
            Ok(true)
        );
        owner
            .mark_ready(&owner.lifecycle_mutation_target().unwrap())
            .unwrap();

        fs::write(root.path().join(".fail-private-backup-cleanup"), b"fail").unwrap();
        {
            let mut guard = owner.inner.writer();
            guard.durable.state = Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded {
                model_id: model_id.clone(),
                revision: model.source.revision.clone(),
                artifact_digest: digest.clone(),
            });
            persist_durable(root.path(), &guard.durable).unwrap();
        }
        assert!(owner.enqueue_demand_acquisition_if_needed());
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            let finished = owner
                .worker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .handle
                .as_ref()
                .is_some_and(JoinHandle::is_finished);
            if finished {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "background replacement did not finish"
            );
            thread::yield_now();
        }
        assert_eq!(
            owner.cancel_and_join_background_acquisition_until(
                std::time::Instant::now() + Duration::from_secs(1),
            ),
            Ok(true)
        );
        assert!(install.is_dir(), "new install must survive backup cleanup failure");
        assert!(matches!(
            owner.status().state,
            Some(SemanticModelLifecycleStateV1::Installed { .. })
        ));
        assert!(crate::LoadableLifecycleArtifactV1::from_state(
            owner.status().state,
            owner.catalog(),
        )
        .is_ok());
        assert!(owner
            .inner
            .read()
            .durable
            .private_install_debts
            .iter()
            .any(|debt| debt
                .install_path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".previous-install-"))));
    }

    #[test]
    fn private_cleanup_removes_adopted_regular_staging_and_quarantine_debt() {
        let root = tempfile::tempdir().unwrap();
        ensure_lifecycle_private_directories(root.path()).unwrap();
        let staging = root
            .path()
            .join("staging/.previous-install-TinyFixtureModel-0123456789abcdef-1-2-3-4");
        let quarantine = root
            .path()
            .join("quarantine/acquisition-1-2-3-4-TinyFixtureModel-0123456789abcdef");
        for path in [&staging, &quarantine] {
            fs::write(path, b"adopted regular-file debt").unwrap();
            assert_eq!(cleanup_private_owned_path(root.path(), path), Ok(()));
            assert!(!path.exists());
        }
    }

    #[test]
    fn cancellation_joins_background_acquisition_without_installing() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let root = tempfile::tempdir().unwrap();
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let source = Arc::new(BlockingFixtureSource {
            root: fixture.path().to_path_buf(),
            calls: AtomicUsize::new(0),
            entered: entered_tx,
            release: Mutex::new(release_rx),
        });
        let owner =
            Arc::new(SemanticModelLifecycleOwnerV1::open(root.path(), catalog, source).unwrap());
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("background acquisition entered fixture source");

        let cancel_owner = Arc::clone(&owner);
        let (joined_tx, joined_rx) = mpsc::sync_channel(1);
        let canceller = thread::spawn(move || {
            joined_tx
                .send(
                    cancel_owner
                        .cancel_and_join_background_acquisition_until(
                            std::time::Instant::now() + Duration::from_secs(1),
                        )
                        .and_then(|joined| {
                            joined
                                .then_some(())
                                .ok_or(ModelLifecycleErrorV1::WorkerJoinFailed)
                        }),
                )
                .expect("report cancellation outcome");
        });
        assert!(
            joined_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "cancellation must join the still-running acquisition worker"
        );
        release_tx.send(()).expect("release fixture source");
        joined_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("joined cancellation outcome")
            .expect("cancel and join background acquisition");
        canceller.join().expect("cancellation caller");

        assert!(
            owner
                .worker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .handle
                .is_none(),
            "joined worker handle must not remain mounted"
        );
        let status = owner.status();
        assert!(matches!(
            status.state.as_ref(),
            Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded { .. })
        ));
        assert!(
            status.remediation.retry,
            "a cancelled acquisition must report a retryable state"
        );
        assert!(
            !matches!(
                status.state.as_ref(),
                Some(
                    SemanticModelLifecycleStateV1::Installed { .. }
                        | SemanticModelLifecycleStateV1::Ready { .. }
                )
            ),
            "cancelled acquisition must never install or ready the model"
        );
    }

    #[test]
    fn cancellation_before_private_publication_preserves_prior_install() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let digest = catalog_package_digest(&model);
        let root = tempfile::tempdir().unwrap();
        let owner = Arc::new(
            SemanticModelLifecycleOwnerV1::open(
                root.path(),
                catalog,
                Arc::new(FixtureSource {
                    root: fixture.path().to_path_buf(),
                    calls: AtomicUsize::new(0),
                }),
            )
            .unwrap(),
        );
        owner.select_model(Some(&model_id), true).unwrap();
        owner.acquire_blocking_for_tests().unwrap();
        let install = install_path_for(root.path(), &model_id, &model.source.revision, &digest);
        assert!(install.is_dir());

        // Force a second acquisition while retaining the first install. The
        // worker will be cancelled before its staging directory can publish
        // over the canonical path.
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let blocking_source = Arc::new(BlockingFixtureSource {
            root: fixture.path().to_path_buf(),
            calls: AtomicUsize::new(0),
            entered: entered_tx,
            release: Mutex::new(release_rx),
        });
        let mut replacement_catalog = owner.catalog().clone();
        let replacement_model = replacement_catalog.get(&model_id).unwrap().clone();
        replacement_catalog
            .models
            .retain(|candidate| candidate.model_id != model_id);
        replacement_catalog.models.push(replacement_model);
        drop(owner);
        let owner = Arc::new(
            SemanticModelLifecycleOwnerV1::open(
                root.path(),
                replacement_catalog,
                blocking_source,
            )
            .unwrap(),
        );
        {
            let mut guard = owner.inner.writer();
            guard.durable.selected_model = Some(model_id.clone());
            guard.durable.state = Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded {
                model_id: model_id.clone(),
                revision: model.source.revision.clone(),
                artifact_digest: digest,
            });
            persist_durable(root.path(), &guard.durable).unwrap();
        }
        assert!(owner.enqueue_demand_acquisition_if_needed());
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("replacement acquisition entered source");

        owner.cancel_background_acquisition();
        let cancelling_owner = Arc::clone(&owner);
        let join = thread::spawn(move || {
            cancelling_owner.cancel_and_join_background_acquisition_until(
                std::time::Instant::now() + Duration::from_secs(1),
            )
        });
        release_tx.send(()).unwrap();
        assert_eq!(join.join().unwrap(), Ok(true));
        assert!(
            install.is_dir(),
            "cancellation before publication must not delete the prior install"
        );
    }

    #[test]
    fn selection_change_supersedes_blocked_acquisition_before_mutation() {
        for next_model in [Some(DEFAULT_FASTEMBED_MODEL_ID), None] {
            let fixture = tempfile::tempdir().unwrap();
            let (catalog, model_id) = tiny_catalog(fixture.path());
            let model = catalog.get(&model_id).unwrap().clone();
            let digest = catalog_package_digest(&model);
            let root = tempfile::tempdir().unwrap();
            let staging = staging_path_for(root.path(), &model_id, &digest);
            let install = install_path_for(root.path(), &model_id, &model.source.revision, &digest);
            let (entered_tx, entered_rx) = mpsc::sync_channel(1);
            let (release_tx, release_rx) = mpsc::sync_channel(1);
            let owner = Arc::new(
                SemanticModelLifecycleOwnerV1::open(
                root.path(),
                catalog,
                Arc::new(BlockingFixtureSource {
                    root: fixture.path().to_path_buf(),
                    calls: AtomicUsize::new(0),
                    entered: entered_tx,
                    release: Mutex::new(release_rx),
                }),
            )
                .unwrap(),
            );
            owner.select_model(Some(&model_id), true).unwrap();
            assert!(owner.enqueue_demand_acquisition_if_needed());
            entered_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("background acquisition entered fixture source");

            // Selection owns the mutation reservation through cancellation and
            // join. Keep the source blocked long enough to prove the caller
            // waits for that join, then release it so the test cannot leave a
            // non-cancellable fixture stranded forever.
            let selecting_owner = Arc::clone(&owner);
            let (selected_tx, selected_rx) = mpsc::sync_channel(1);
            let selecting = thread::spawn(move || {
                selected_tx
                    .send(selecting_owner.select_model(next_model, false))
                    .expect("selection result receiver");
            });
            assert!(
                selected_rx.recv_timeout(Duration::from_millis(50)).is_err(),
                "selection must join the blocked acquisition before publication"
            );
            release_tx.send(()).expect("release fixture source");
            selected_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("selection result")
                .expect("selection");
            selecting.join().expect("selection caller");
            assert_eq!(
                owner.cancel_and_join_background_acquisition_until(
                    std::time::Instant::now() + Duration::from_secs(1),
                ),
                Ok(true)
            );
            assert!(
                !staging.exists(),
                "stale worker staging must be removed after the owner joins it"
            );
            assert!(
                !install.exists(),
                "stale worker private install must be removed after the owner joins it"
            );

            let status = owner.status();
            assert_eq!(status.selected_model.as_deref(), next_model);
            assert!(
                next_model.is_some_and(|model| {
                    matches!(
                        status.state.as_ref(),
                        Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded {
                            model_id,
                            ..
                        }) if model_id == model
                    )
                }) || next_model.is_none() && status.state.is_none(),
                "superseded acquisition must not overwrite the new selection: {status:?}"
            );
        }
    }

    #[test]
    fn selection_change_removes_a_completed_private_install() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let digest = catalog_package_digest(&model);
        let root = tempfile::tempdir().unwrap();
        let staging = staging_path_for(root.path(), &model_id, &digest);
        let install = install_path_for(root.path(), &model_id, &model.source.revision, &digest);
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());

        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            let installed = matches!(
                owner.status().state,
                Some(SemanticModelLifecycleStateV1::Installed { .. })
            );
            let finished = owner
                .worker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .handle
                .as_ref()
                .is_some_and(JoinHandle::is_finished);
            if install.exists() && installed && finished {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "background acquisition did not publish a finished private install"
            );
            thread::yield_now();
        }

        owner.select_model(None, false).unwrap();
        assert_eq!(
            owner.cancel_and_join_background_acquisition_until(
                std::time::Instant::now() + Duration::from_secs(1),
            ),
            Ok(true)
        );
        assert!(!staging.exists());
        assert!(!install.exists());
        assert!(owner.status().state.is_none());
    }

    #[test]
    fn disabling_semantics_clears_a_rollback_pointer_to_the_retired_private_install() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let root = tempfile::tempdir().unwrap();
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        join_background_acquisition(&owner).unwrap();
        owner
            .mark_ready(&owner.lifecycle_mutation_target().unwrap())
            .unwrap();
        let ready = owner.status().state.clone().unwrap();
        {
            let mut guard = owner.inner.writer();
            guard.durable.previous_ready = Some(ready);
            persist_durable(root.path(), &guard.durable).unwrap();
        }

        let disabled = owner.select_model(None, false).unwrap();
        assert!(disabled.state.is_none());
        assert!(!disabled.remediation.rollback);
        assert_eq!(
            owner.rollback_to_previous(),
            Err(ModelLifecycleErrorV1::Rejected),
            "disabling must not leave a rollback pointer to deleted bytes"
        );
    }

    #[test]
    fn private_cleanup_debt_survives_restart_without_follow_up_persistence() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let digest = catalog_package_digest(&model);
        let root = tempfile::tempdir().unwrap();
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog.clone(),
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        join_background_acquisition(&owner).unwrap();
        let install = install_path_for(root.path(), &model_id, &model.source.revision, &digest);
        assert!(install.is_dir());

        // Inject a cleanup failure while keeping the original path stable. The
        // replacement state and ownership metadata have already been persisted
        // before retirement is attempted.
        fs::remove_dir_all(&install).unwrap();
        fs::write(&install, b"private cleanup collision").unwrap();
        fs::write(root.path().join(".fail-private-owned-cleanup"), b"fail").unwrap();
        let cleanup_error = owner
            .select_model(None, false)
            .expect_err("private cleanup must report the collision");
        assert_eq!(
            cleanup_error,
            ModelLifecycleErrorV1::CancellationCleanupFailed(install.clone())
        );
        drop(owner);

        let reopened = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        assert!(reopened.status().remediation.remove);
        assert_eq!(
            reopened
                .inner
                .read()
                .durable
                .private_install
                .as_ref()
                .map(|private| private.install_path.clone()),
            Some(install.clone()),
            "restart must recover the durable private cleanup owner"
        );

        let removed = reopened.remove_install().unwrap();
        assert!(removed.state.is_none());
        assert!(!removed.remediation.remove);
        assert!(!install.exists(), "remove_install must remove a regular-file debt after restart");
    }

    #[test]
    fn private_cleanup_debt_survives_progress_and_reacquisition() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let digest = catalog_package_digest(&model);
        let root = tempfile::tempdir().unwrap();
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog.clone(),
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        join_background_acquisition(&owner).unwrap();
        let install = install_path_for(root.path(), &model_id, &model.source.revision, &digest);
        fs::remove_dir_all(&install).unwrap();
        fs::write(&install, b"private cleanup collision").unwrap();
        fs::write(root.path().join(".fail-private-owned-cleanup"), b"fail").unwrap();

        assert_eq!(
            owner.select_model(None, false).unwrap_err(),
            ModelLifecycleErrorV1::CancellationCleanupFailed(install.clone())
        );
        assert_eq!(
            owner
                .inner
                .read()
                .durable
                .private_install_debts
                .iter()
                .map(|debt| debt.install_path.clone())
                .collect::<Vec<_>>(),
            vec![install.clone()]
        );

        // Reacquire a different private model and exercise every runtime
        // projection state. The replacement must be a verified install so the
        // lifecycle target admission check proves the cleanup debt survives
        // real progress rather than accepting a fabricated path.
        let alternate_id = "TinyFixtureModelTwo".to_owned();
        let mut alternate = model.clone();
        alternate.model_id = alternate_id.clone();
        alternate.backend = CatalogedEmbeddingBackendV1::FastEmbedOrt {
            fastembed_enum: alternate_id.clone(),
        };
        alternate.model_code = "tracedecay/tiny-fixture-two".to_owned();
        alternate.source.revision =
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_owned();
        alternate.source.provenance = format!(
            "https://example.invalid/tracedecay/tiny-fixture-two/tree/{}",
            alternate.source.revision
        );
        let mut catalog_with_alternate = catalog.clone();
        catalog_with_alternate.models.push(alternate.clone());
        // Reopen with the expanded catalog so selection and acquisition use
        // the alternate private path while the original debt remains present.
        drop(owner);
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog_with_alternate.clone(),
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        // Keep the old path unresolved while the alternate selection is
        // published. The first injected failure was consumed by the restart
        // recovery path above, so arm the cleanup fault again at this
        // replacement boundary.
        fs::write(root.path().join(".fail-private-owned-cleanup"), b"fail").unwrap();
        assert_eq!(
            owner.select_model(Some(&alternate_id), true).unwrap_err(),
            ModelLifecycleErrorV1::CancellationCleanupFailed(install.clone())
        );
        assert!(owner.enqueue_demand_acquisition_if_needed());
        join_background_acquisition(&owner).unwrap();
        let alternate_digest = catalog_package_digest(&alternate);
        let alternate_install = install_path_for(
            root.path(),
            &alternate_id,
            &alternate.source.revision,
            &alternate_digest,
        );
        assert!(alternate_install.exists());
        assert!(owner
            .inner
            .read()
            .durable
            .private_install_debts
            .iter()
            .any(|debt| debt.install_path == install));

        let target = owner.lifecycle_mutation_target().unwrap();
        owner.mark_loading(&target).unwrap();
        let target = owner.lifecycle_mutation_target().unwrap();
        owner.mark_indexing(&target, 1, 1).unwrap();
        let target = owner.lifecycle_mutation_target().unwrap();
        owner.mark_ready(&target).unwrap();
        let target = owner.lifecycle_mutation_target().unwrap();
        owner
            .mark_runtime_failed(&target, "runtime progress", true)
            .unwrap();
        assert!(owner
            .inner
            .read()
            .durable
            .private_install_debts
            .iter()
            .any(|debt| debt.install_path == install));

        // Then publish a shared import; the unresolved old private owner must
        // remain durable across both replacement forms and restart.

        owner.select_model(Some(&model_id), false).unwrap();
        owner
            .import_local_artifact(&model_id, &tiny_manifest(&model), fixture.path(), 10)
            .unwrap();
        assert!(owner
            .inner
            .read()
            .durable
            .private_install_debts
            .iter()
            .any(|debt| debt.install_path == install));
        drop(owner);

        let reopened = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog_with_alternate,
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        assert!(reopened
            .inner
            .read()
            .durable
            .private_install_debts
            .iter()
            .any(|debt| debt.install_path == install));
        fs::remove_file(&install).unwrap();
        reopened.remove_install().unwrap();
        assert!(reopened
            .inner
            .read()
            .durable
            .private_install_debts
            .is_empty());
    }

    #[test]
    fn private_backup_cleanup_failure_is_retained_across_restart() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let root = tempfile::tempdir().unwrap();
        let source = Arc::new(FixtureSource {
            root: fixture.path().to_path_buf(),
            calls: AtomicUsize::new(0),
        });
        let owner = SemanticModelLifecycleOwnerV1::open(root.path(), catalog.clone(), source.clone())
            .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        owner.acquire_blocking_for_tests().unwrap();
        owner
            .mark_ready(&owner.lifecycle_mutation_target().unwrap())
            .unwrap();
        fs::write(root.path().join(".fail-private-backup-cleanup"), b"fail").unwrap();

        let failure = owner
            .acquire_blocking_for_tests()
            .expect_err("backup cleanup failure must fail the replacement");
        assert_eq!(failure, ModelLifecycleErrorV1::InstallFailed);
        let backup_path = {
            let guard = owner.inner.read();
            guard
                .durable
                .private_install_debts
                .iter()
                .find(|debt| {
                    debt.install_path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.starts_with(".previous-install-"))
                })
                .map(|debt| debt.install_path.clone())
                .expect("backup path is durable cleanup debt")
        };
        assert!(backup_path.exists());
        drop(owner);

        let reopened = SemanticModelLifecycleOwnerV1::open(root.path(), catalog, source).unwrap();
        assert!(reopened
            .inner
            .read()
            .durable
            .private_install_debts
            .iter()
            .any(|debt| debt.install_path == backup_path));
        reopened.remove_install().unwrap();
        assert!(!backup_path.exists());
        assert!(reopened
            .inner
            .read()
            .durable
            .private_install_debts
            .is_empty());
    }

    #[test]
    fn startup_reconciles_a_private_backup_left_before_debt_persistence() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let digest = catalog_package_digest(&model);
        let root = tempfile::tempdir().unwrap();
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog.clone(),
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        owner.acquire_blocking_for_tests().unwrap();
        let install = install_path_for(root.path(), &model_id, &model.source.revision, &digest);
        let backup = private_backup_path_for(root.path(), &model_id, &digest, 1);
        fs::rename(&install, &backup).unwrap();
        drop(owner);

        // This models a process exit after the atomic rename and before the
        // follow-up lifecycle debt write. The moved install.json is enough to
        // recover the exact owner metadata on the next open.
        let reopened = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        assert!(reopened
            .inner
            .read()
            .durable
            .private_install_debts
            .iter()
            .any(|debt| debt.install_path == backup));
        assert!(backup.exists());
        reopened.remove_install().unwrap();
        assert!(!backup.exists());
    }

    #[test]
    fn a_restart_or_revision_never_reuses_and_deletes_an_existing_backup() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let digest = catalog_package_digest(&model);
        let root = tempfile::tempdir().unwrap();
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        owner.acquire_blocking_for_tests().unwrap();
        let install = install_path_for(root.path(), &model_id, &model.source.revision, &digest);
        let legacy_backup = root
            .path()
            .join("staging")
            .join(format!(".previous-install-{model_id}-1"));
        fs::create_dir_all(&legacy_backup).unwrap();
        fs::write(legacy_backup.join("sentinel"), b"retain me").unwrap();

        owner.acquire_blocking_for_tests().unwrap();
        assert!(install.exists());
        assert_eq!(
            fs::read(legacy_backup.join("sentinel")).unwrap(),
            b"retain me"
        );
    }

    #[test]
    fn startup_does_not_adopt_an_unrecognized_staging_directory_with_dashes() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, _model_id) = tiny_catalog(fixture.path());
        let root = tempfile::tempdir().unwrap();
        let unrelated = root.path().join("staging").join("user-notes-with-dashes");
        fs::create_dir_all(&unrelated).unwrap();
        fs::write(unrelated.join("sentinel"), b"retain me").unwrap();

        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();

        assert!(unrelated.exists());
        assert!(!owner
            .inner
            .read()
            .durable
            .private_install_debts
            .iter()
            .any(|debt| debt.install_path == unrelated));
    }

    #[test]
    fn same_model_replacement_identity_rejects_a_stale_runtime_commit() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let root = tempfile::tempdir().unwrap();
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        owner.acquire_blocking_for_tests().unwrap();
        let stale_target = owner.lifecycle_mutation_target().unwrap();
        let projection = crate::session_pool::test_support::authority()
            .projection()
            .clone()
            .with_lifecycle_artifact_identity(stale_target.artifact_digest());
        assert_eq!(
            owner.lifecycle_mutation_target_for_projection(&projection),
            Some(stale_target.clone())
        );

        {
            let mut guard = owner.inner.writer();
            let state = guard.durable.state.clone().unwrap();
            guard.durable.state = Some(SemanticModelLifecycleStateV1::Installed {
                model_id: state.model_id().to_owned(),
                revision: state_revision(&state).to_owned(),
                artifact_digest: "same-model-replacement".to_owned(),
                install_path: install_path_of(&state).unwrap().to_path_buf(),
            });
            persist_durable(root.path(), &guard.durable).unwrap();
        }
        assert!(owner
            .lifecycle_mutation_target_for_projection(&projection)
            .is_none());
        let callback_called = AtomicBool::new(false);
        assert_eq!(
            owner.commit_runtime_ready(&stale_target, || {
                callback_called.store(true, Ordering::SeqCst);
                true
            }),
            Err(ModelLifecycleErrorV1::Rejected)
        );
        assert!(!callback_called.load(Ordering::SeqCst));
    }

    #[test]
    fn runtime_commit_holds_the_mutation_reservation_through_ready_publication() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let root = tempfile::tempdir().unwrap();
        let owner = Arc::new(
            SemanticModelLifecycleOwnerV1::open(
                root.path(),
                catalog,
                Arc::new(FixtureSource {
                    root: fixture.path().to_path_buf(),
                    calls: AtomicUsize::new(0),
                }),
            )
            .unwrap(),
        );
        owner.select_model(Some(&model_id), true).unwrap();
        owner.acquire_blocking_for_tests().unwrap();
        let target = owner.lifecycle_mutation_target().unwrap();
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let committing_owner = Arc::clone(&owner);
        let commit_thread = thread::spawn(move || {
            committing_owner.commit_runtime_ready(&target, || {
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                true
            })
        });
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("runtime commit entered its linearization window");

        let selecting_owner = Arc::clone(&owner);
        let (selected_tx, selected_rx) = mpsc::sync_channel(1);
        let selecting = thread::spawn(move || {
            selected_tx
                .send(selecting_owner.select_model(None, false))
                .unwrap();
        });
        assert!(
            selected_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "selection must wait for the runtime commit reservation"
        );
        release_tx.send(()).unwrap();
        assert_eq!(commit_thread.join().unwrap(), Ok(true));
        selected_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("selection result")
            .unwrap();
        selecting.join().unwrap();
    }

    #[test]
    fn same_model_selection_preserves_an_unreaped_successful_private_install() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let digest = catalog_package_digest(&model);
        let root = tempfile::tempdir().unwrap();
        let install = install_path_for(root.path(), &model_id, &model.source.revision, &digest);
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());

        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            let installed = matches!(
                owner.status().state,
                Some(SemanticModelLifecycleStateV1::Installed { .. })
            );
            let unreaped = owner
                .worker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .handle
                .as_ref()
                .is_some_and(JoinHandle::is_finished);
            if installed && unreaped {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "background acquisition did not publish an unreaped private install"
            );
            thread::yield_now();
        }

        owner
            .select_model(Some(&model_id), true)
            .expect("same-model selection");
        assert!(
            install.exists(),
            "same-model selection must preserve the published private install"
        );
        assert!(matches!(
            owner.status().state,
            Some(SemanticModelLifecycleStateV1::Installed { .. })
        ));
        assert!(
            !owner.enqueue_demand_acquisition_if_needed(),
            "the preserved install must remain admissible without reacquisition"
        );
    }

    #[test]
    fn shutdown_preserves_an_unreaped_successful_private_install() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let digest = catalog_package_digest(&model);
        let root = tempfile::tempdir().unwrap();
        let install = install_path_for(root.path(), &model_id, &model.source.revision, &digest);
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());

        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            let installed = matches!(
                owner.status().state,
                Some(SemanticModelLifecycleStateV1::Installed { .. })
            );
            let unreaped = owner
                .worker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .handle
                .as_ref()
                .is_some_and(JoinHandle::is_finished);
            if installed && unreaped {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "background acquisition did not publish an unreaped private install"
            );
            thread::yield_now();
        }

        assert_eq!(
            owner.cancel_and_join_background_acquisition_until(
                std::time::Instant::now() + Duration::from_secs(1),
            ),
            Ok(true)
        );
        assert!(
            install.exists(),
            "shutdown join must preserve the published private install"
        );
        assert!(matches!(
            owner.status().state,
            Some(SemanticModelLifecycleStateV1::Installed { .. })
        ));
        assert!(matches!(
            owner
                .select_model(Some(&model_id), true)
                .expect("re-admit after shutdown")
                .state,
            Some(SemanticModelLifecycleStateV1::Installed { .. })
        ));
    }

    #[test]
    fn remove_install_reaps_and_removes_a_completed_private_install_under_reservation() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let digest = catalog_package_digest(&model);
        let root = tempfile::tempdir().unwrap();
        let install = install_path_for(root.path(), &model_id, &model.source.revision, &digest);
        let owner = Arc::new(
            SemanticModelLifecycleOwnerV1::open(
                root.path(),
                catalog,
                Arc::new(FixtureSource {
                    root: fixture.path().to_path_buf(),
                    calls: AtomicUsize::new(0),
                }),
            )
            .unwrap(),
        );
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());

        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            let installed = matches!(
                owner.status().state,
                Some(SemanticModelLifecycleStateV1::Installed { .. })
            );
            let finished = owner
                .worker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .handle
                .as_ref()
                .is_some_and(JoinHandle::is_finished);
            if install.exists() && installed && finished {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "background acquisition did not publish a finished private install"
            );
            thread::yield_now();
        }

        {
            let mut guard = owner.inner.writer();
            guard.durable.previous_ready = guard.durable.state.clone();
            persist_durable(root.path(), &guard.durable).unwrap();
        }
        let after_join_entered = Arc::new(Barrier::new(2));
        let after_join_release = Arc::new(Barrier::new(2));
        owner.set_mutation_pause_after_join_for_tests(
            Arc::clone(&after_join_entered),
            Arc::clone(&after_join_release),
        );
        let removing_owner = Arc::clone(&owner);
        let (remove_tx, remove_rx) = mpsc::sync_channel(1);
        let remover = thread::spawn(move || {
            remove_tx
                .send(removing_owner.remove_install())
                .expect("remove result receiver");
        });
        let _ = after_join_entered.wait();

        let selecting_owner = Arc::clone(&owner);
        let (select_tx, select_rx) = mpsc::sync_channel(1);
        let selecting = thread::spawn(move || {
            select_tx
                .send(selecting_owner.select_model(None, false))
                .expect("selection result receiver");
        });
        assert!(
            select_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "selection must wait while remove_install owns the post-join reservation"
        );
        let _ = after_join_release.wait();

        let removed = remove_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("remove result")
            .expect("remove install");
        remover.join().expect("remove caller");
        assert!(matches!(
            removed.state,
            Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded { .. })
        ));
        assert!(!removed.remediation.rollback);
        assert!(
            !install.exists(),
            "remove_install must remove the completed worker private install"
        );
        assert!(
            select_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("selection result")
                .expect("selection")
                .state
                .is_none()
        );
        selecting.join().expect("selection caller");
        assert_eq!(
            owner.rollback_to_previous(),
            Err(ModelLifecycleErrorV1::Rejected),
            "remove_install must clear a rollback pointer to deleted bytes"
        );
    }

    #[test]
    fn explicit_import_joins_active_acquisition_before_publication() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let root = tempfile::tempdir().unwrap();
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let owner = Arc::new(
            SemanticModelLifecycleOwnerV1::open(
                root.path(),
                catalog,
                Arc::new(BlockingFixtureSource {
                    root: fixture.path().to_path_buf(),
                    calls: AtomicUsize::new(0),
                    entered: entered_tx,
                    release: Mutex::new(release_rx),
                }),
            )
            .unwrap(),
        );
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("background acquisition entered fixture source");

        let after_join_entered = Arc::new(Barrier::new(2));
        let after_join_release = Arc::new(Barrier::new(2));
        owner.set_mutation_pause_after_join_for_tests(
            Arc::clone(&after_join_entered),
            Arc::clone(&after_join_release),
        );

        let import_owner = Arc::clone(&owner);
        let import_model_id = model_id.clone();
        let import_manifest = tiny_manifest(&model);
        let import_source = fixture.path().to_path_buf();
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let importer = thread::spawn(move || {
            result_tx
                .send(import_owner.import_local_artifact(
                    &import_model_id,
                    &import_manifest,
                    &import_source,
                    10,
                ))
                .expect("explicit import result receiver");
        });
        assert!(
            result_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "explicit import must join the active acquisition first"
        );
        release_tx.send(()).expect("release fixture source");
        let _ = after_join_entered.wait();

        let demand_owner = Arc::clone(&owner);
        let (demand_tx, demand_rx) = mpsc::sync_channel(1);
        let demand = thread::spawn(move || {
            demand_tx
                .send(demand_owner.enqueue_demand_acquisition_if_needed())
                .expect("demand result receiver");
        });
        assert!(
            demand_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "demand must wait while import owns the post-join mutation reservation"
        );
        let _ = after_join_release.wait();

        let imported = result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("explicit import result")
            .expect("explicit import");
        importer.join().expect("explicit import caller");
        assert!(matches!(
            imported.state,
            Some(SemanticModelLifecycleStateV1::Installed { .. })
        ));
        assert!(matches!(
            owner.status().state,
            Some(SemanticModelLifecycleStateV1::Installed { .. })
        ));
        assert!(
            owner
                .worker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .handle
                .is_none(),
            "explicit import must not leave the cancelled worker mounted"
        );
        assert_eq!(
            demand_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("demand result"),
            false,
            "demand after import publication must observe the installed model"
        );
        demand.join().expect("demand caller");
    }

    #[test]
    fn explicit_import_removes_a_finished_worker_private_install() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let digest = catalog_package_digest(&model);
        let root = tempfile::tempdir().unwrap();
        let install = install_path_for(root.path(), &model_id, &model.source.revision, &digest);
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());

        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            let installed = matches!(
                owner.status().state,
                Some(SemanticModelLifecycleStateV1::Installed { .. })
            );
            let finished = owner
                .worker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .handle
                .as_ref()
                .is_some_and(JoinHandle::is_finished);
            if install.exists() && installed && finished {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "background acquisition did not publish a finished private install"
            );
            thread::yield_now();
        }

        let stale_target = owner.lifecycle_mutation_target().unwrap();
        let imported = owner
            .import_local_artifact(&model_id, &tiny_manifest(&model), fixture.path(), 10)
            .expect("explicit import after finished acquisition");
        assert!(
            !install.exists(),
            "explicit import must retire the completed worker private install"
        );
        assert!(matches!(
            imported.state,
            Some(SemanticModelLifecycleStateV1::Installed { .. })
        ));
        assert_eq!(
            owner.mark_ready(&stale_target),
            Err(ModelLifecycleErrorV1::Rejected),
            "importing a replacement artifact must invalidate the prior projection target"
        );
    }

    #[test]
    fn private_selection_persist_failure_keeps_prior_install_admissible() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let digest = catalog_package_digest(&model);
        let root = tempfile::tempdir().unwrap();
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        join_background_acquisition(&owner).unwrap();
        let install = install_path_for(root.path(), &model_id, &model.source.revision, &digest);
        assert!(install.exists());

        fs::remove_file(root.path().join("lifecycle.json")).unwrap();
        fs::create_dir(root.path().join("lifecycle.json")).unwrap();
        assert_eq!(
            owner.select_model(None, false).unwrap_err(),
            ModelLifecycleErrorV1::StoreUnavailable
        );
        assert!(
            install.exists(),
            "failed selection must retain private bytes"
        );
        assert_eq!(
            owner.status().selected_model.as_deref(),
            Some(model_id.as_str())
        );
        assert!(matches!(
            owner.status().state,
            Some(SemanticModelLifecycleStateV1::Installed { .. })
        ));
    }

    #[test]
    fn private_import_persist_failure_keeps_prior_install_admissible() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let digest = catalog_package_digest(&model);
        let root = tempfile::tempdir().unwrap();
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        join_background_acquisition(&owner).unwrap();
        let install = install_path_for(root.path(), &model_id, &model.source.revision, &digest);
        assert!(install.exists());

        fs::remove_file(root.path().join("lifecycle.json")).unwrap();
        fs::create_dir(root.path().join("lifecycle.json")).unwrap();
        assert_eq!(
            owner
                .import_local_artifact(&model_id, &tiny_manifest(&model), fixture.path(), 10)
                .unwrap_err(),
            ModelLifecycleErrorV1::StoreUnavailable
        );
        assert!(install.exists(), "failed import must retain private bytes");
        assert!(matches!(
            owner.status().state,
            Some(SemanticModelLifecycleStateV1::Installed { .. })
        ));
    }

    #[test]
    fn private_rollback_persist_failure_keeps_previous_ready_install() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let digest = catalog_package_digest(&model);
        let root = tempfile::tempdir().unwrap();
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        join_background_acquisition(&owner).unwrap();
        owner
            .mark_ready(&owner.lifecycle_mutation_target().unwrap())
            .unwrap();
        let install = install_path_for(root.path(), &model_id, &model.source.revision, &digest);
        let ready = owner.status().state.clone().unwrap();
        {
            let mut guard = owner.inner.writer();
            guard.durable.previous_ready = Some(ready.clone());
            guard.durable.state = Some(SemanticModelLifecycleStateV1::Failed {
                model_id: model_id.clone(),
                revision: model.source.revision.clone(),
                artifact_digest: digest,
                detail: "injected runtime failure".to_owned(),
                retryable: true,
            });
            persist_durable(root.path(), &guard.durable).unwrap();
        }

        fs::remove_file(root.path().join("lifecycle.json")).unwrap();
        fs::create_dir(root.path().join("lifecycle.json")).unwrap();
        assert_eq!(
            owner.rollback_to_previous().unwrap_err(),
            ModelLifecycleErrorV1::StoreUnavailable
        );
        assert!(
            install.exists(),
            "failed rollback must retain private bytes"
        );
        assert!(matches!(
            owner.status().state,
            Some(SemanticModelLifecycleStateV1::Failed { .. })
        ));
        assert!(matches!(owner.status().remediation.rollback, true));
    }

    #[test]
    fn private_acquisition_persist_failure_restores_prior_install_and_state() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let digest = catalog_package_digest(&model);
        let root = tempfile::tempdir().unwrap();
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog.clone(),
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        let install = install_path_for(root.path(), &model_id, &model.source.revision, &digest);
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            let installed = matches!(
                owner.status().state,
                Some(SemanticModelLifecycleStateV1::Installed { .. })
            );
            let finished = owner
                .worker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .handle
                .as_ref()
                .is_some_and(JoinHandle::is_finished);
            if installed && finished && install.exists() {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "initial private acquisition did not finish"
            );
            thread::yield_now();
        }
        assert_eq!(
            owner.cancel_and_join_background_acquisition_until(
                std::time::Instant::now() + Duration::from_secs(1),
            ),
            Ok(true)
        );
        let prior_install_metadata = fs::read(install.join("install.json")).unwrap();
        owner
            .mark_runtime_failed(
                &owner.lifecycle_mutation_target().unwrap(),
                "retry after runtime failure",
                true,
            )
            .unwrap();

        // The production background path publishes a new private install only
        // after the durable Installed write. Force that write to fail so the
        // worker must restore both the prior bytes and its prior ownership.
        fs::write(root.path().join(".fail-installed-lifecycle-persist"), b"fail").unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            let finished = owner
                .worker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .handle
                .as_ref()
                .is_some_and(JoinHandle::is_finished);
            if finished {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "persist-failure acquisition did not finish"
            );
            thread::yield_now();
        }
        assert_eq!(
            owner.cancel_and_join_background_acquisition_until(
                std::time::Instant::now() + Duration::from_secs(1),
            ),
            Ok(true)
        );
        assert_eq!(
            fs::read(install.join("install.json")).unwrap(),
            prior_install_metadata,
            "failed Installed publication must restore the prior private bytes"
        );
        assert!(matches!(
            owner.status().state,
            Some(SemanticModelLifecycleStateV1::Failed {
                retryable: true,
                ..
            })
        ));
        assert_eq!(
            owner
                .inner
                .read()
                .durable
                .private_install
                .as_ref()
                .map(|private| private.install_path.clone()),
            Some(install.clone()),
            "failed publication must restore prior in-memory ownership"
        );
        let durable_bytes = fs::read(root.path().join("lifecycle.json")).unwrap();
        drop(owner);

        let reopened = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        assert!(install.is_dir());
        assert!(matches!(
            reopened.status().state,
            Some(SemanticModelLifecycleStateV1::Failed {
                retryable: true,
                ..
            })
        ));
        assert_eq!(
            reopened
                .inner
                .read()
                .durable
                .private_install
                .as_ref()
                .map(|private| private.install_path.clone()),
            Some(install)
        );
        assert_eq!(
            fs::read(root.path().join("lifecycle.json")).unwrap(),
            durable_bytes,
            "reopen must observe the same recovered durable ownership"
        );
    }

    #[test]
    fn rollback_rejects_a_missing_previous_private_install() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let digest = catalog_package_digest(&model);
        let root = tempfile::tempdir().unwrap();
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        join_background_acquisition(&owner).unwrap();
        owner
            .mark_ready(&owner.lifecycle_mutation_target().unwrap())
            .unwrap();
        let ready = owner.status().state.clone().unwrap();
        let install = install_path_for(root.path(), &model_id, &model.source.revision, &digest);
        {
            let mut guard = owner.inner.writer();
            guard.durable.previous_ready = Some(ready);
            guard.durable.state = Some(SemanticModelLifecycleStateV1::Failed {
                model_id: model_id.clone(),
                revision: model.source.revision.clone(),
                artifact_digest: digest,
                detail: "injected runtime failure".to_owned(),
                retryable: true,
            });
            persist_durable(root.path(), &guard.durable).unwrap();
        }
        fs::remove_dir_all(&install).unwrap();

        assert_eq!(
            owner.rollback_to_previous(),
            Err(ModelLifecycleErrorV1::Rejected),
            "rollback must refuse a previous private install whose directory is gone"
        );
        assert!(matches!(
            owner.status().state,
            Some(SemanticModelLifecycleStateV1::Failed { .. })
        ));
    }

    #[test]
    fn rollback_joins_active_acquisition_before_publication() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let digest = catalog_package_digest(&model);
        let root = tempfile::tempdir().unwrap();
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let owner = Arc::new(
            SemanticModelLifecycleOwnerV1::open(
                root.path(),
                catalog,
                Arc::new(BlockingFixtureSource {
                    root: fixture.path().to_path_buf(),
                    calls: AtomicUsize::new(0),
                    entered: entered_tx,
                    release: Mutex::new(release_rx),
                }),
            )
            .unwrap(),
        );
        owner
            .import_local_artifact(&model_id, &tiny_manifest(&model), fixture.path(), 10)
            .unwrap();
        owner
            .mark_ready(&owner.lifecycle_mutation_target().unwrap())
            .unwrap();
        let previous = owner.status().state.clone().unwrap();
        {
            let mut guard = owner.inner.writer();
            guard.durable.previous_ready = Some(previous);
            guard.durable.state = Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded {
                model_id: model.model_id.clone(),
                revision: model.source.revision.clone(),
                artifact_digest: digest,
            });
            persist_durable(root.path(), &guard.durable).unwrap();
        }
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("background acquisition entered fixture source");

        let rollback_owner = Arc::clone(&owner);
        let (result_tx, result_rx) = mpsc::sync_channel(1);
        let after_join_entered = Arc::new(Barrier::new(2));
        let after_join_release = Arc::new(Barrier::new(2));
        owner.set_mutation_pause_after_join_for_tests(
            Arc::clone(&after_join_entered),
            Arc::clone(&after_join_release),
        );
        let rollback = thread::spawn(move || {
            result_tx
                .send(rollback_owner.rollback_to_previous())
                .expect("rollback result receiver");
        });
        assert!(
            result_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "rollback must join the active acquisition first"
        );
        release_tx.send(()).expect("release fixture source");
        let _ = after_join_entered.wait();

        let demand_owner = Arc::clone(&owner);
        let (demand_tx, demand_rx) = mpsc::sync_channel(1);
        let demand = thread::spawn(move || {
            demand_tx
                .send(demand_owner.enqueue_demand_acquisition_if_needed())
                .expect("demand result receiver");
        });
        assert!(
            demand_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "demand must wait while rollback owns the post-join mutation reservation"
        );
        let _ = after_join_release.wait();

        let rolled_back = result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("rollback result")
            .expect("rollback");
        rollback.join().expect("rollback caller");
        assert!(matches!(
            rolled_back.state,
            Some(SemanticModelLifecycleStateV1::Ready { .. })
        ));
        assert!(matches!(
            owner.status().state,
            Some(SemanticModelLifecycleStateV1::Ready { .. })
        ));
        assert!(
            owner
                .worker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .handle
                .is_none(),
            "rollback must not leave the cancelled worker mounted"
        );
        assert_eq!(
            demand_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("demand result"),
            false,
            "demand after rollback publication must observe the ready model"
        );
        demand.join().expect("demand caller");
    }

    #[test]
    fn automatic_spawn_revalidates_auto_download_under_worker_lock() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let root = tempfile::tempdir().unwrap();
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        owner.select_model(Some(&model_id), false).unwrap();

        assert!(
            !owner.spawn_acquire(true, Some(&model_id)),
            "automatic acquisition must revalidate the selection's auto-download policy"
        );
        assert!(
            !owner.spawn_acquire(false, Some(DEFAULT_FASTEMBED_MODEL_ID)),
            "explicit retry must not acquire a selection other than the requested model"
        );
    }

    #[test]
    fn retryable_background_worker_failure_is_retried_by_later_demand() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let member_count = catalog.get(&model_id).unwrap().members.len();
        let root = tempfile::tempdir().unwrap();
        let source = Arc::new(FailOncePanickingFixtureSource {
            root: fixture.path().to_path_buf(),
            calls: AtomicUsize::new(0),
        });
        let owner = SemanticModelLifecycleOwnerV1::open(root.path(), catalog, source.clone()).unwrap();
        owner.select_model(Some(&model_id), true).unwrap();

        assert!(owner.enqueue_demand_acquisition_if_needed());
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        loop {
            if owner
                .worker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .handle
                .as_ref()
                .is_some_and(JoinHandle::is_finished)
            {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "fail-once acquisition worker did not finish"
            );
            thread::yield_now();
        }
        assert_eq!(source.calls.load(Ordering::SeqCst), 1);

        assert!(
            owner.enqueue_demand_acquisition_if_needed(),
            "a later strict demand must consume the retryable worker outcome"
        );
        join_background_acquisition(&owner).expect("retry acquisition must complete");
        assert!(matches!(
            owner.status().state,
            Some(SemanticModelLifecycleStateV1::Installed { .. })
        ));
        assert_eq!(
            source.calls.load(Ordering::SeqCst),
            member_count + 1,
            "the retry must fetch every member after the fail-once attempt"
        );
    }

    #[test]
    fn retry_holds_mutation_reservation_across_selection_and_spawn() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let root = tempfile::tempdir().unwrap();
        let owner = Arc::new(
            SemanticModelLifecycleOwnerV1::open(
                root.path(),
                catalog,
                Arc::new(FailOncePanickingFixtureSource {
                    root: fixture.path().to_path_buf(),
                    calls: AtomicUsize::new(0),
                }),
            )
            .unwrap(),
        );
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while !owner
            .worker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .handle
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
        {
            assert!(
                std::time::Instant::now() < deadline,
                "fail-once acquisition worker did not finish"
            );
            thread::yield_now();
        }

        let after_join_entered = Arc::new(Barrier::new(2));
        let after_join_release = Arc::new(Barrier::new(2));
        owner.set_mutation_pause_after_join_for_tests(
            Arc::clone(&after_join_entered),
            Arc::clone(&after_join_release),
        );
        let retry_owner = Arc::clone(&owner);
        let (retry_tx, retry_rx) = mpsc::sync_channel(1);
        let retry = thread::spawn(move || {
            retry_tx
                .send(retry_owner.retry())
                .expect("retry result receiver");
        });
        let _ = after_join_entered.wait();

        let selecting_owner = Arc::clone(&owner);
        let (select_tx, select_rx) = mpsc::sync_channel(1);
        let selecting = thread::spawn(move || {
            select_tx
                .send(selecting_owner.select_model(None, false))
                .expect("selection result receiver");
        });
        assert!(
            select_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "selection must wait while retry owns its select/spawn reservation"
        );
        let _ = after_join_release.wait();

        retry_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("retry result")
            .expect("retry");
        retry.join().expect("retry caller");
        assert!(matches!(
            select_rx
                .recv_timeout(Duration::from_secs(1))
                .expect("selection result")
                .expect("selection")
                .selected_model,
            None
        ));
        selecting.join().expect("selection caller");
        assert!(owner.status().selected_model.is_none());
        assert!(owner.status().state.is_none());
        assert_eq!(
            owner.cancel_and_join_background_acquisition_until(
                std::time::Instant::now() + Duration::from_secs(1),
            ),
            Ok(true)
        );
    }

    #[test]
    fn retry_normalizes_a_finished_worker_panic_before_remediation() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let root = tempfile::tempdir().unwrap();
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(FailOncePanickingFixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while !owner
            .worker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .handle
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
        {
            assert!(
                std::time::Instant::now() < deadline,
                "fail-once acquisition worker did not finish"
            );
            thread::yield_now();
        }
        assert!(
            !owner.status().remediation.retry,
            "an unreaped worker panic must expose its interrupted durable state"
        );

        owner
            .retry()
            .expect("retry must reap and normalize the panic");
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while !owner
            .worker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .handle
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
        {
            assert!(
                std::time::Instant::now() < deadline,
                "retry acquisition worker did not finish"
            );
            thread::yield_now();
        }
        assert_eq!(
            owner.cancel_and_join_background_acquisition_until(
                std::time::Instant::now() + Duration::from_secs(1),
            ),
            Ok(true)
        );
        assert!(matches!(
            owner.status().state,
            Some(SemanticModelLifecycleStateV1::Installed { .. })
        ));
    }

    #[test]
    fn cancelled_background_state_restarts_after_orphaned_download() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let root = tempfile::tempdir().unwrap();
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let source = Arc::new(BlockingFixtureSource {
            root: fixture.path().to_path_buf(),
            calls: AtomicUsize::new(0),
            entered: entered_tx,
            release: Mutex::new(release_rx),
        });
        let owner = SemanticModelLifecycleOwnerV1::open(root.path(), catalog.clone(), source).unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("background acquisition entered fixture source");

        owner.cancel_background_acquisition();
        release_tx.send(()).expect("release fixture source");
        assert_eq!(
            join_background_acquisition(&owner),
            Err(ModelLifecycleErrorV1::Cancelled)
        );
        assert!(matches!(
            owner.status().state,
            Some(SemanticModelLifecycleStateV1::Downloading { .. })
        ));
        drop(owner);

        let restarted_source = Arc::new(FixtureSource {
            root: fixture.path().to_path_buf(),
            calls: AtomicUsize::new(0),
        });
        let restarted =
            SemanticModelLifecycleOwnerV1::open(root.path(), catalog, restarted_source).unwrap();
        let status = restarted.status();
        assert!(matches!(
            status.state,
            Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded { .. })
        ));
        assert!(status.remediation.retry, "orphaned state must be retryable");
        assert!(status.semantics_omitted);
        assert!(restarted.enqueue_demand_acquisition_if_needed());
        join_background_acquisition(&restarted).expect("orphaned acquisition must resume");
        assert!(matches!(
            restarted.status().state,
            Some(SemanticModelLifecycleStateV1::Installed { .. })
        ));
    }

    #[test]
    fn orphaned_verifying_state_restarts_as_retryable_download() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let root = tempfile::tempdir().unwrap();
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog.clone(),
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        {
            let mut guard = owner.inner.writer();
            guard.durable.state = Some(SemanticModelLifecycleStateV1::Verifying {
                model_id: model.model_id.clone(),
                revision: model.source.revision.clone(),
                artifact_digest: catalog_package_digest(&model),
            });
            persist_durable(&owner.root, &guard.durable).unwrap();
        }
        drop(owner);

        let restarted = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(FixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
            }),
        )
        .unwrap();
        let status = restarted.status();
        assert!(matches!(
            status.state,
            Some(SemanticModelLifecycleStateV1::SelectedNotDownloaded { .. })
        ));
        assert!(status.remediation.retry);
        assert!(restarted.enqueue_demand_acquisition_if_needed());
        join_background_acquisition(&restarted).expect("orphaned verification must resume");
        assert!(matches!(
            restarted.status().state,
            Some(SemanticModelLifecycleStateV1::Installed { .. })
        ));
    }

    #[test]
    fn stale_worker_outcome_is_discarded_after_selection_change_before_shutdown() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let root = tempfile::tempdir().unwrap();
        let owner =
            SemanticModelLifecycleOwnerV1::open(root.path(), catalog, Arc::new(PanickingFixtureSource))
                .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while !owner
            .worker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .handle
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
        {
            assert!(
                std::time::Instant::now() < deadline,
                "panicking acquisition worker did not finish"
            );
            thread::yield_now();
        }

        owner.select_model(None, true).unwrap();
        assert!(owner.status().state.is_none());
        assert_eq!(
            owner.cancel_and_join_background_acquisition_until(
                std::time::Instant::now() + Duration::from_secs(1),
            ),
            Ok(true)
        );
        assert_eq!(owner.resolve_background_acquisition_outcome(), None);
    }

    #[test]
    fn stale_cleanup_failure_survives_selection_change_until_resolved() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let root = tempfile::tempdir().unwrap();
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(BlockingFixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
                entered: entered_tx,
                release: Mutex::new(release_rx),
            }),
        )
        .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("background acquisition entered fixture source");

        let digest = catalog_package_digest(&model);
        let staging = root.path().join("staging").join(format!(
            "{}-{}",
            model.model_id,
            &digest[..16.min(digest.len())]
        ));
        fs::remove_dir_all(&staging).unwrap();
        fs::write(&staging, b"staging collision").unwrap();

        owner.cancel_background_acquisition();
        release_tx.send(()).expect("release fixture source");
        let cleanup_error = owner
            .cancel_and_join_background_acquisition_until(
                std::time::Instant::now() + Duration::from_secs(1),
            )
            .expect_err("cleanup failure must be retained");
        assert!(matches!(
            cleanup_error,
            ModelLifecycleErrorV1::CancellationCleanupQuarantined(_)
                | ModelLifecycleErrorV1::CancellationCleanupFailed(_)
        ));
        let quarantine_path = match &cleanup_error {
            ModelLifecycleErrorV1::CancellationCleanupQuarantined(path) => Some(path.clone()),
            ModelLifecycleErrorV1::CancellationCleanupFailed(_) => None,
            _ => unreachable!(),
        };

        let changed = owner
            .select_model(Some(DEFAULT_FASTEMBED_MODEL_ID), true)
            .unwrap();
        assert_eq!(
            changed.selected_model.as_deref(),
            Some(DEFAULT_FASTEMBED_MODEL_ID)
        );
        assert!(changed.remediation.retry);
        assert_eq!(owner.retry(), Err(cleanup_error.clone()));

        owner.select_model(None, false).unwrap();
        assert!(owner.status().state.is_none());
        assert_eq!(
            owner.cancel_and_join_background_acquisition_until(
                std::time::Instant::now() + Duration::from_secs(1),
            ),
            Err(cleanup_error.clone())
        );
        assert_eq!(
            owner.resolve_background_acquisition_outcome(),
            Some(cleanup_error)
        );
        if let Some(quarantine_path) = quarantine_path {
            assert!(
                !quarantine_path.exists(),
                "resolving cleanup debt must remove the quarantine path"
            );
        }
        assert_eq!(
            owner.cancel_and_join_background_acquisition_until(
                std::time::Instant::now() + Duration::from_secs(1),
            ),
            Ok(true)
        );
    }

    #[test]
    fn panic_cleanup_failure_preserves_join_metadata_for_retry_after_resolution() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let model = catalog.get(&model_id).unwrap().clone();
        let root = tempfile::tempdir().unwrap();
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let owner = SemanticModelLifecycleOwnerV1::open(
            root.path(),
            catalog,
            Arc::new(BlockingFailOncePanickingFixtureSource {
                root: fixture.path().to_path_buf(),
                calls: AtomicUsize::new(0),
                entered: entered_tx,
                release: Mutex::new(release_rx),
            }),
        )
        .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("background acquisition entered fixture source");

        let digest = catalog_package_digest(&model);
        let staging = root.path().join("staging").join(format!(
            "{}-{}",
            model.model_id,
            &digest[..16.min(digest.len())]
        ));
        fs::remove_dir_all(&staging).unwrap();
        fs::write(&staging, b"staging collision").unwrap();

        release_tx.send(()).expect("release fixture source");
        let cleanup_error = owner
            .cancel_and_join_background_acquisition_until(
                std::time::Instant::now() + Duration::from_secs(1),
            )
            .expect_err("panic cleanup failure must be retained");
        assert!(matches!(
            cleanup_error,
            ModelLifecycleErrorV1::CancellationCleanupQuarantined(_)
                | ModelLifecycleErrorV1::CancellationCleanupFailed(_)
        ));

        // Retire the selection while the cleanup debt remains. Resolving the
        // current debt must promote the retained panic with its original
        // selection metadata, so the next selection can fence it as stale.
        owner.select_model(None, false).unwrap();
        assert_eq!(
            owner.resolve_background_acquisition_outcome(),
            Some(cleanup_error.clone())
        );
        {
            let worker = owner.worker.lock().unwrap_or_else(PoisonError::into_inner);
            assert_eq!(
                worker.outcome,
                Some(ModelLifecycleErrorV1::WorkerJoinFailed)
            );
            assert_eq!(worker.outcome_model_id.as_deref(), Some(model_id.as_str()));
            assert!(worker.outcome_selection_generation.is_some());
        }

        owner.select_model(Some(&model_id), true).unwrap();
        assert!(
            owner
                .worker
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .outcome
                .is_none(),
            "selection change must discard the stale promoted panic"
        );
        owner.retry().expect("retry after cleanup resolution");
        join_background_acquisition(&owner).expect("retry acquisition must complete");
        assert!(matches!(
            owner.status().state,
            Some(SemanticModelLifecycleStateV1::Installed { .. })
        ));
    }

    #[test]
    fn terminal_worker_join_outcome_is_retained_across_shutdown_retries() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let root = tempfile::tempdir().unwrap();
        let owner =
            SemanticModelLifecycleOwnerV1::open(root.path(), catalog, Arc::new(PanickingFixtureSource))
        .unwrap();
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while !owner
            .worker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .handle
            .as_ref()
            .is_some_and(JoinHandle::is_finished)
        {
            assert!(
                std::time::Instant::now() < deadline,
                "panicking acquisition worker did not finish"
            );
            thread::yield_now();
        }

        for _ in 0..2 {
            assert_eq!(
                owner.cancel_and_join_background_acquisition_until(
                    std::time::Instant::now() + Duration::from_secs(1),
                ),
                Err(ModelLifecycleErrorV1::WorkerJoinFailed),
            );
        }
        assert_eq!(
            owner.resolve_background_acquisition_outcome(),
            Some(ModelLifecycleErrorV1::WorkerJoinFailed),
        );
        assert_eq!(
            owner.cancel_and_join_background_acquisition_until(
                std::time::Instant::now() + Duration::from_secs(1),
            ),
            Ok(true),
        );
    }

    #[test]
    fn concurrent_shutdown_retries_cannot_report_clean_while_join_is_pending() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let root = tempfile::tempdir().unwrap();
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let owner = Arc::new(
            SemanticModelLifecycleOwnerV1::open(
                root.path(),
                catalog,
                Arc::new(BlockingPanickingFixtureSource {
                    entered: entered_tx,
                    release: Mutex::new(release_rx),
                }),
            )
            .unwrap(),
        );
        owner.select_model(Some(&model_id), true).unwrap();
        assert!(owner.enqueue_demand_acquisition_if_needed());
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("background acquisition entered fixture source");

        let start = Arc::new(Barrier::new(3));
        let (result_tx, result_rx) = mpsc::sync_channel(2);
        let callers = (0..2)
            .map(|_| {
                let owner = Arc::clone(&owner);
                let start = Arc::clone(&start);
                let result_tx = result_tx.clone();
                thread::spawn(move || {
                    start.wait();
                    result_tx
                        .send(owner.cancel_and_join_background_acquisition_until(
                            std::time::Instant::now() + Duration::from_secs(1),
                        ))
                        .expect("shutdown result receiver");
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        assert!(
            result_rx.recv_timeout(Duration::from_millis(50)).is_err(),
            "no shutdown caller may report clean while its peer is joining the worker"
        );
        release_tx.send(()).expect("release fixture source");
        for _ in 0..2 {
            assert_eq!(
                result_rx
                    .recv_timeout(Duration::from_secs(1))
                    .expect("shutdown result"),
                Err(ModelLifecycleErrorV1::WorkerJoinFailed),
            );
        }
        for caller in callers {
            caller.join().expect("shutdown caller");
        }
    }

    #[test]
    fn cancellation_returns_typed_cancelled_without_publishing_failed_state() {
        let fixture = tempfile::tempdir().unwrap();
        let (catalog, model_id) = tiny_catalog(fixture.path());
        let root = tempfile::tempdir().unwrap();
        let (entered_tx, entered_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let owner = Arc::new(
            SemanticModelLifecycleOwnerV1::open(
                root.path(),
                catalog,
                Arc::new(BlockingFixtureSource {
                    root: fixture.path().to_path_buf(),
                    calls: AtomicUsize::new(0),
                    entered: entered_tx,
                    release: Mutex::new(release_rx),
                }),
            )
            .unwrap(),
        );
        owner.select_model(Some(&model_id), false).unwrap();
        let acquisition_owner = Arc::clone(&owner);
        let acquisition = thread::spawn(move || acquisition_owner.acquire_blocking_for_tests());
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("blocking acquisition entered fixture source");

        owner.cancel_background_acquisition();
        release_tx.send(()).expect("release fixture source");

        assert_eq!(
            acquisition.join().expect("acquisition worker"),
            Err(ModelLifecycleErrorV1::Cancelled),
        );
        assert!(!matches!(
            owner.status().state,
            Some(SemanticModelLifecycleStateV1::Failed { .. }),
        ));
    }

    #[test]
    fn cancellation_and_installed_publication_are_serialized_by_epoch() {
        let control = Arc::new(AcquisitionControlV1::default());
        let epoch = control.begin_epoch();
        let (publication_entered_tx, publication_entered_rx) = mpsc::sync_channel(1);
        let (release_publication_tx, release_publication_rx) = mpsc::sync_channel(1);
        let installed = Arc::new(AtomicBool::new(false));
        let installed_by_publication = Arc::clone(&installed);
        let publication = thread::spawn(move || {
            epoch.while_active(|| {
                publication_entered_tx
                    .send(())
                    .expect("publication-entered receiver");
                release_publication_rx
                    .recv()
                    .expect("release-publication sender");
                installed_by_publication.store(true, Ordering::SeqCst);
                Ok(())
            })
        });
        publication_entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("publication owns epoch");

        let cancellation_control = Arc::clone(&control);
        let (cancelled_tx, cancelled_rx) = mpsc::sync_channel(1);
        let cancellation = thread::spawn(move || {
            cancellation_control.cancel_current();
            cancelled_tx.send(()).expect("cancelled receiver");
        });
        assert!(
            cancelled_rx
                .recv_timeout(Duration::from_millis(50))
                .is_err(),
            "cancellation must wait while Installed publication owns the epoch"
        );
        release_publication_tx
            .send(())
            .expect("release publication");
        publication
            .join()
            .expect("publication worker")
            .expect("Installed publication");
        cancelled_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("cancellation completed");
        cancellation.join().expect("cancellation worker");
        assert!(installed.load(Ordering::SeqCst));

        let cancelled_epoch = control.begin_epoch();
        control.cancel_current();
        assert_eq!(
            cancelled_epoch.while_active(|| {
                installed.store(false, Ordering::SeqCst);
                Ok(())
            }),
            Err(ModelLifecycleErrorV1::Cancelled),
        );
        assert!(installed.load(Ordering::SeqCst));
    }
