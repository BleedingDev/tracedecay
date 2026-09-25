# NCM session-open readiness fixture draft

Frozen draft only: `open-session-readiness.patch`, SHA-256 `e4bfb578524a03dc99a65d48e3a7f4da2904ad7fd9624f3cbd5bb9dd05da6060`.

cc1391's only failed common case never invoked its privacy Recall. The report records the final handshake expecting generation 0 but returning `stale_identity` with generation 2. Earlier rows successfully populated the admitted destination, visited empty foreign scopes, then reopened the populated destination before updating the host source disposition.

`OpenSession` recreates the host and production surface. The new surface initially declares generation 0. `RustNcmSurface::handshake` installs the actual persisted generation and returns `StaleIdentity` when the discovered generation differs; this preserves honest readiness identity. The fixture's restart callback already resolves that explicit transition through `reopen_persisted_namespace`, with at most one stale-only retry using the same finite request control.

This draft reuses that existing helper in `OpenSession`. It changes no production code, shared case, expected authority refusal, original/cached grant, source selection, foreign-scope check, or conformance handshake assertion. The next common operation still requires a successful fully checked handshake and evaluates the original privacy assertion. Other failures remain failures.

Validation: proposed file passes rustfmt; patch applies cleanly; live source still equals captured baseline. No Cargo, worker, or model run. Root can rerun the unchanged real common suite after reviewing and applying the draft.
