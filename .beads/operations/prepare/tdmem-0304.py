#!/usr/bin/env python3
"""Materialize tdmem-0304 against the accepted provider API."""

from __future__ import annotations

import subprocess
from pathlib import Path

HERE = Path(__file__).resolve()
ROOT = HERE.parents[3]
SOURCE_COMMIT = "c3d9e1e6fd3d807828a4b373be14b6f0da3db848"
SOURCE_PATH = ".beads/operations/prepare/tdmem-0304.py"
BODY = HERE.with_name("_tdmem-0304-body.py")

result = subprocess.run(
    ["git", "show", f"{SOURCE_COMMIT}:{SOURCE_PATH}"],
    cwd=ROOT,
    check=True,
    capture_output=True,
    text=True,
)
source = result.stdout

unused_import = (
    "    OwnedExactScope, OwnedOpaqueExtension, OwnedProviderId, OwnedVersionedId, ProviderCall,\n"
)
fixed_import = (
    "    OwnedExactScope, OwnedOpaqueExtension, OwnedVersionedId, ProviderCall,\n"
)
if source.count(unused_import) != 1:
    raise SystemExit("could not locate the unused NCM provider-ID import")
source = source.replace(unused_import, fixed_import, 1)

old_invoke_failure = '''    fn invoke_failure(
        call: &ProviderCall,
        code: TerminalCode,
        diagnostic_id: &'static str,
    ) -> ProviderReply {
        let mut reply = ProviderReply::failure(call, code);
        reply.terminal.diagnostic_id = Some(diagnostic_id.to_owned());
        reply
    }
'''
new_invoke_failure = '''    fn invoke_failure(
        call: &ProviderCall,
        code: TerminalCode,
        diagnostic_id: &'static str,
    ) -> ProviderReply {
        let scope = NcmNamespace::from_exact_scope(&call.exact_scope);
        let terminal = match TerminalRecord::new(
            code,
            CommittedEffectState::None,
            FallbackEligibility::Forbidden,
            call.operation_id.clone(),
            scope.as_str(),
            None,
            Some(diagnostic_id.to_owned()),
        ) {
            Ok(value) => value,
            Err(_) => TerminalRecord {
                terminal_code: TerminalCode::InternalFailure,
                committed_effect: CommittedEffectState::None,
                fallback: FallbackEligibility::Forbidden,
                operation_id: call.operation_id.clone(),
                exact_scope_sha256: scope.0,
                provider_receipt_sha256: None,
                diagnostic_id: Some("ncm.adapter_terminal_construction_failed".to_owned()),
            },
        };
        ProviderReply {
            terminal,
            payload: None,
            warnings: Vec::new(),
            extensions: Vec::new(),
            state_generation: call.expected_state_generation,
        }
    }
'''
if source.count(old_invoke_failure) != 1:
    raise SystemExit("could not locate the stale NCM invoke-failure helper")
source = source.replace(old_invoke_failure, new_invoke_failure, 1)

stale_scope = "                call.exact_scope_sha256(),\n"
fixed_scope = (
    "                NcmNamespace::from_exact_scope(&call.exact_scope).as_str(),\n"
)
if source.count(stale_scope) != 2:
    raise SystemExit("could not locate both stale exact-scope calls")
source = source.replace(stale_scope, fixed_scope)

if source.count("        let mut terminal = if call.operation.mutates_provider_state() {") != 1:
    raise SystemExit("could not locate the stale mutable terminal result")
source = source.replace(
    "        let mut terminal = if call.operation.mutates_provider_state() {",
    "        let terminal = if call.operation.mutates_provider_state() {",
    1,
)
if source.count("        let terminal = match terminal.take() {") != 1:
    raise SystemExit("could not locate the stale Result::take call")
source = source.replace(
    "        let terminal = match terminal.take() {",
    "        let terminal = match terminal {",
    1,
)

undocumented_tests = "TESTS = r'''use std::collections::BTreeSet;\n"
documented_tests = (
    "TESTS = r'''//! Focused integration tests for the topology-neutral NCM adapter boundary.\n"
    "#![allow(clippy::expect_used)]\n\n"
    "use std::collections::BTreeSet;\n"
)
if source.count(undocumented_tests) != 1:
    raise SystemExit("could not locate the undocumented NCM integration test crate")
source = source.replace(undocumented_tests, documented_tests, 1)

BODY.write_text(source, encoding="utf-8")
subprocess.run(["python3", str(BODY)], cwd=ROOT, check=True)
HERE.unlink()
