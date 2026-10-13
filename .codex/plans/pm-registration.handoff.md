# pm-registration integration handoff

Implemented source began in the registry/configuration ownership assignment. Root reviewed and explicitly approved applying `pm-registration.integration.patch` to `project_composition.rs` and `project_composition/ncm_observer.rs`; that exact patch is now APPLIED. Standalone rustfmt and scoped diff checks pass. Root owns compilation, publication and subsequent authority injection; no Cargo, model, worker or operator action was run by this worker.

## Registration API

`ProviderRegistrationV1` injects `provider_id`, the concrete `Arc<dyn MemoryProvider>`, positive `registration_revision`, `mode`, `execution_shape`, host-admitted `recall_scope_bindings`, and `lifecycle`.

`ProjectMemoryProviderComposition::compose_registered` accepts `SelectedProviderActivationV1::Injected { fabric_config, registration }`, `ObserversOnly { fabric_config }`, or `Disabled`, plus rich observer registrations. Legacy Native and observer constructors remain compatibility wrappers. New active injection requires `memory.advisory_common.v1`, all its required capabilities and nonempty authorized recall bindings. The profile is rechecked against the descriptor retained by the existing fabric, whose limits are retained as registration evidence. The registry does not recognize provider names on this path, create another route, change modes or revisions, or fall back to Native. Observers never receive recall scope authorization, including the legacy selected Native-observer variant.

`registry.selected_registration()` returns metadata with provider identity/revision/mode/actual limits/execution shape. It returns none for an observer-only set. `registry.registration(id)` supplies the bound provider metadata for readiness. Fabric health, revision, mode and routing remain authoritative.

`ProviderLifecycleOwnershipV1::Owned(Arc<dyn ProviderLifecycleOwnerV1>)` uses the adapter's existing runtime owner. Methods are `start(i64) -> Result<()>`, `request_stop(i64) -> Result<bool>`, and `kill(i64) -> Result<()>`, with UTC-microsecond deadlines and `ProviderLifecycleOwnerErrorV1::{DeadlineElapsed, TerminationUnconfirmed, Unavailable(String)}`. A true stop or successful kill confirms actual termination. `CompositionBound` means no distinct provider incarnation; retiring readiness does not kill or prove completion of host invocation threads. The invocation boundary retains its stranded-worker accounting.

Per-exact-scope readiness now constructs `CompositionLifecycleAdapterV1::for_provider`, selecting the lifecycle owner for the actual scope provider, including observers. The supervisor state machine is unchanged.

`ConfiguredObservationProviderMountV1 { mount, requirement, activation }` uses independent `ObservationMountRequirementV1::{Required, Optional}` and `ObservationMountActivationV1::{BeforePublication, AfterPublication}`. Required failure policy never implies bootstrap timing. Mount/start loops use this metadata rather than position or provider identity.

## Existing configuration

No key, serialized shape, registry schema revision or default changes. `MemoryProviderSelectionV1::resolve(native_enabled, &ncm_observer, &routing)` returns independent Native/NCM `MemoryProviderParticipationV1::{Disabled, Observer, Active}` and typed selection failures. `active_provider()` returns `MemoryProviderKindV1::{Native,Ncm}`; `is_disabled()` avoids constructing any infrastructure. Unknown and disabled active selections fail without enabling another provider. The existing NCM `mode: enabled` value means participation: it observes unless the existing routing document explicitly selects `ncm`. `MemoryProviderNcmParticipationV1` is a descriptive alias for the existing type.

Canonical facts remain the host service; no facts or provenance code changed.

## Applied root patch

`pm-registration.integration.patch` contains exact changes to `project_composition.rs` and its `ncm_observer.rs` constructor, plus the directly affected root routing unit tests. It:

- Replaces the Native-only activation table with domain selection and fails enabled configuration when the host feature is absent.
- Constructs only enabled adapters; selected-adapter construction failure is fatal, optional-observer failure remains visible and independent.
- Uses the injected common-profile registration path and actual selected metadata for route identity/revision/limits.
- Reuses `NcmWorkerOwnerSlot::acquire` and the same `Arc<RustNcmWorkerOwner>` for adapter and owner control. The NCM worker owner now exposes bounded `Instant` methods via the NCM worker lane. No new invocation-state slot or manifest dependency is required.
- Binds mandatory observation/replay through explicit metadata. Required Native mounts preserve startup replay before publication; optional Native and every NCM mount remain dormant before publication, including selected required NCM. Only deferred journeys are carried through `PublishedFullServer` and activated in `finish_full_server` after registry cutover; the old `.skip(1)` is removed. The same owned journey is activated once, with no duplicate start or replacement owner.
- Keeps the old NCM observer constructor for existing test harnesses under `cfg(test)`.

NCM's execution shape is `HostAuthoredInProcess`: the synchronous adapter wrapper is workspace-owned cooperative code running on a host thread. Its separately owned worker process is cancellable/terminable, but this does not make that wrapper thread a foreign-code hard-kill boundary.

The patch only connects registration/limits/lifecycle composition. Full active host recall provenance, temporal/history admission and product journeys remain later owners. Existing adversarial Native interpositions that advertise legacy-only descriptors will need the real common-profile fixture before they can enter the new product registration path; do not weaken production profile admission for those fixtures.

## Tests for root

- `tracedecay-memory-provider-registry --test selected_registration`: injected NCM without Native, actual metadata, identity/profile failures, compatible arbitrary identity, observer output isolation, observer-only ownership, confirmed versus unconfirmed termination, disabled infrastructure, execution/observer-mode rejection.
- Existing registry `--test registry`: legacy Native construction and no recall bindings in observer mode.
- Existing registry `--test supervised_readiness` and `--test supervisor`: unchanged state machine with bound ownership selection.
- `tracedecay-domain` filter `memory_provider_selection_tests`: default wire compatibility, independent NCM active/observer participation, disabled/unknown failures.
- `tracedecay-configuration` filter `legacy_participation_snapshot_selects_ncm_without_enabling_native`.
- `tracedecay-global-db` filter `memory_provider_registration_tests`: same project scope, defaults and daemon-restart metadata.
- `tracedecay-global-db` filter `selected_ncm_configuration_persists_with_native_disabled_and_pinned_revisions`: stored settings, immutable parent, reopen, no needless convergence revision, pending restart and no worker/state construction.

Registry reexports the requested existing advisory values and common profile constants, including `AdvisoryAdmissionAuthority`, `AdvisoryAdmissionError`, `CurrentAdvisoryAdmission`, `CurrentRestoreAdmission`, `GrantedHistorySource`, `CurrentSourceDisposition`, and `LifecycleTargetReference`. Generated `UnknownValidityPolicy` is exported as `CommonUnknownValidityPolicy` because the existing admission type already owns `UnknownValidityPolicy`.

## Approved publication failure policy

A required deferred activation failure returns `TraceDecayError::Config` from `finish_full_server` before dependent owners and `mark_publication_ready`. Its caller already retains `Some(server)` on this error. The existing `settle_failed_full_upgrade` funnel reclaims the core by identity-checked `reclaim_core_after_failed_upgrade`, marks publication failed, revokes responses on the failed full server, and uses `schedule_project_server_retirement` for tracked shutdown of all its mounted journeys. If reclaim fails or cancellation owns the open, `retire_failed_project_open_owner` retires the published owners and returns the error.

This proposal preserves the existing deliberate core-degradation policy: when core reclaim succeeds, the open returns the core after logging `full_upgrade_degraded`; it does not leave the failed full server published or claim the selected provider started. Root approved this exact existing degradation/reclaim/retirement policy before the patch was applied. Optional activation failure retains its warning path. Returning a bare construction error after publication would lose the failed server identity, so deferred activation now lives in the existing `finish_full_server` failure funnel.

## Review corrections and validation checkpoint

Root ran cc-1315: 110 of 111 tests passed, including all injected-selection tests. The sole failure was the disabled-composition supervisor regression. The fix now confirms no predecessor only when the composition itself is `Disabled`; start/handshake remain unavailable, and missing providers in enabled composition still fail. A new direct regression verifies that enabled-but-absent identities cannot claim stop/kill success. Ten selected-registration tests now exist; the latest fix/test awaits root execution. No worker-run Cargo command was used.
