#!/usr/bin/env python3
"""Retry tdmem-0302 against the canonical committed-effect enum."""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
BODY = Path(__file__).with_name("_tdmem-0302-body.py")
SOURCE_COMMIT = "0d83ca7ea1b70bbdc86bbfa9998949c217597e2c"
SOURCE_PATH = ".beads/operations/prepare/tdmem-0302.py"

source = subprocess.check_output(
    ["git", "show", f"{SOURCE_COMMIT}:{SOURCE_PATH}"],
    cwd=ROOT,
    text=True,
)
source = source.replace(
    "CommittedEffectState::Applied",
    "CommittedEffectState::Committed",
)
source = source.replace(
    '''            Entry::Vacant(entry) if at_capacity => {
                let _ = entry;
                Err(FabricError::RegistryCapacityExhausted)
            }
''',
    '''            Entry::Vacant(_) if at_capacity => {
                Err(FabricError::RegistryCapacityExhausted)
            }
''',
)
BODY.write_text(source, encoding="utf-8")
subprocess.run(["python3", str(BODY)], cwd=ROOT, check=True)
Path(__file__).unlink()
