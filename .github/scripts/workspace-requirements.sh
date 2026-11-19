#!/usr/bin/env bash
set -euo pipefail

metadata() {
  if [ -n "${1:-}" ]; then
    cat "$1"
  else
    cargo metadata --no-deps --format-version 1
  fi
}

mismatched=$(metadata "${1:-}" | jq -r '
  (.packages | map({key: .name, value: .version}) | from_entries) as $versions
  | .packages[] | .name as $from
  | .dependencies[]
  | select(.path != null and .req != "*" and $versions[.name] != null)
  | select(.req != "^" + $versions[.name] and .req != "=" + $versions[.name])
  | "\($from) requires \(.name) \(.req), and the workspace builds \(.name) \($versions[.name])"')

if [ -n "$mismatched" ]; then
  while IFS= read -r line; do
    echo "::error::$line"
  done <<<"$mismatched"
  exit 1
fi
echo "every workspace requirement names the version the workspace builds"
