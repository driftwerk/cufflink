#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."
script=.github/scripts/workspace-requirements.sh
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

workspace() {
  jq -n --arg req "$1" '{packages: [
    {name: "cufflink-types", version: "0.23.0", dependencies: []},
    {name: "cufflink-cli", version: "0.23.0", dependencies: [
      {name: "cufflink-types", req: $req, path: "/repo/types"},
      {name: "cufflink-secrets", req: "*", path: "/repo/secrets"},
      {name: "serde", req: "^1.0", path: null}
    ]}
  ]}' >"$work/metadata.json"
}

failures=0

expect() {
  local case="$1" status="$2" line="$3" output got=0
  shift 3
  output=$("$script" "$@" 2>&1) || got=$?
  if [ "$got" = "$status" ] && grep -qF -- "$line" <<<"$output"; then
    echo "ok: $case"
  else
    printf 'FAILED: %s (exit %s, expected %s with "%s")\n%s\n' \
      "$case" "$got" "$status" "$line" "$output"
    failures=$((failures + 1))
  fi
}

workspace "^0.23.0"
expect "a caret requirement on the version built" 0 "every workspace requirement" "$work/metadata.json"

workspace "=0.23.0"
expect "an exact requirement on the version built" 0 "every workspace requirement" "$work/metadata.json"

workspace "^0.22.2-rc.1"
expect "a requirement left behind by a bump" 1 \
  "cufflink-cli requires cufflink-types ^0.22.2-rc.1, and the workspace builds cufflink-types 0.23.0" \
  "$work/metadata.json"

expect "this workspace" 0 "every workspace requirement"

[ "$failures" = 0 ] || { echo "$failures case(s) failed" >&2; exit 1; }
