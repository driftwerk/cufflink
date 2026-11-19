#!/usr/bin/env bash
set -euo pipefail

metadata() {
  if [ -n "${1:-}" ]; then
    cat "$1"
  else
    cargo metadata --no-deps --format-version 1
  fi
}

checked=$(metadata "${1:-}" | jq -c '
  (.packages | map({key: .name, value: .version}) | from_entries) as $versions
  | [.packages[] | .name as $from | .dependencies[]
     | select($versions[.name] != null and (.path == null or .req != "*"))
     | {from: $from, name, as: (.rename // .name), req, path, source, built: $versions[.name]}]
  | {
      compared: length,
      refused: map(
        if .path == null then
          "\(.from) requires \(.as) \(.req) from \(.source // "outside the workspace"), not the \(.name) \(.built) the workspace builds"
        elif .req != "^" + .built and .req != "=" + .built then
          "\(.from) requires \(.as) \(.req), and the workspace builds \(.name) \(.built)"
        else empty end)
    }')

compared=$(jq -r '.compared' <<<"$checked")
if [ "$compared" = 0 ]; then
  echo "::error::no requirement on a workspace crate was found to compare"
  exit 1
fi
jq -r '.refused[] | "::error::\(.)"' <<<"$checked"
if jq -e '.refused | length > 0' <<<"$checked" >/dev/null; then
  exit 1
fi
echo "each of the $compared requirements on a workspace crate names the version the workspace builds"
