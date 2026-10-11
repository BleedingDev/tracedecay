#!/usr/bin/env bash
# Usage: check-dashboard-digests.sh DIR
#
# DIR holds one dashboard-sha256-<target>.txt per release target, each the
# //dashboard:bundle_sha256 digest that target's binary embeds. Every target
# must embed the same bundle.
set -euo pipefail

dir=${1:?usage: check-dashboard-digests.sh DIR}
shopt -s nullglob
files=("$dir"/dashboard-sha256-*.txt)
distinct=()
for file in "${files[@]}"; do
  # Windows Python writes CRLF.
  digest=$(tr -d '\r\n' <"$file")
  if [[ ! $digest =~ ^[0-9a-f]{64}$ ]]; then
    echo "${file##*/}: not a dashboard bundle digest: '${digest}'" >&2
    exit 1
  fi
  [[ " ${distinct[*]} " == *" $digest "* ]] || distinct+=("$digest")
done

if ((${#distinct[@]} > 1)); then
  echo "release targets embed different dashboard bundles:" >&2
  for file in "${files[@]}"; do
    echo "  ${file##*/}: $(tr -d '\r\n' <"$file")" >&2
  done
  exit 1
fi
echo "${#files[@]} release targets embed dashboard ${distinct[0]:-none}"
