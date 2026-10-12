#!/bin/sh
set -eu

test "$1" = --state-root || exit 2
export TRACEDECAY_NCM_CANCELLATION_FIXTURE_ROOT="$2"

# The state root owns this test's runner; stdout remains the worker wire.
exec "$2/test-runner" --exact enabled::cancellation::worker_fixture \
    --ignored --nocapture 3>&1 1>&2
