#!/bin/sh
set -eu

test "$1" = --state-root || exit 2
export TRACEDECAY_NCM_MALFORMED_FIXTURE_ROOT="$2"

# Keep libtest output separate from the supervised worker's reply stream.
exec "$2/test-runner" --exact malformed_reply_worker_fixture \
    --ignored --nocapture 3>&1 1>&2
