#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."
script="$PWD/.github/scripts/workspace-requirements.sh"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

workspace() {
  local inherited="$1" section="$2" dependency="$3" root="$work/workspace"
  rm -rf "$root"
  mkdir -p "$root/types/src" "$root/cli/src"
  printf '[workspace]\nmembers = ["types", "cli"]\nresolver = "2"\n\n[workspace.dependencies]\n%s\n' \
    "$inherited" >"$root/Cargo.toml"
  printf '[package]\nname = "cufflink-types"\nversion = "0.23.0"\nedition = "2021"\n' \
    >"$root/types/Cargo.toml"
  printf '[package]\nname = "cufflink-cli"\nversion = "0.23.0"\nedition = "2021"\n\n[%s]\n%s\n' \
    "$section" "$dependency" >"$root/cli/Cargo.toml"
  touch "$root/types/src/lib.rs" "$root/cli/src/lib.rs"
  cargo metadata --no-deps --format-version 1 --manifest-path "$root/Cargo.toml" \
    >"$work/metadata.json"
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

inherits='cufflink-types = { version = "0.23.0", path = "types" }'

workspace "$inherits" dependencies 'cufflink-types = { workspace = true }'
expect "a requirement inherited from the workspace on the version built" 0 \
  "each of the 1 requirements" "$work/metadata.json"

workspace "" dependencies 'cufflink-types = { version = "=0.23.0", path = "../types" }'
expect "an exact requirement on the version built" 0 "each of the 1 requirements" \
  "$work/metadata.json"

workspace 'cufflink-types = { version = "0.22.2-rc.1", path = "types" }' dependencies \
  'cufflink-types = { workspace = true }'
expect "an inherited requirement left behind by a bump" 1 \
  "cufflink-cli requires cufflink-types ^0.22.2-rc.1, and the workspace builds cufflink-types 0.23.0" \
  "$work/metadata.json"

workspace "" dependencies 'types = { package = "cufflink-types", version = "0.22", path = "../types" }'
expect "a renamed requirement on another version" 1 \
  "cufflink-cli requires types ^0.22, and the workspace builds cufflink-types 0.23.0" \
  "$work/metadata.json"

workspace "" dev-dependencies 'cufflink-types = { version = "0.22", path = "../types" }'
expect "a dev requirement on another version" 1 \
  "cufflink-cli requires cufflink-types ^0.22, and the workspace builds cufflink-types 0.23.0" \
  "$work/metadata.json"

workspace "" dependencies 'cufflink-types = "0.23.0"'
expect "a workspace crate required from a registry" 1 \
  "cufflink-cli requires cufflink-types ^0.23.0 from a registry" "$work/metadata.json"

workspace "" dependencies 'cufflink-types = { path = "../types" }'
expect "nothing to compare" 1 "no requirement on a workspace crate was found" \
  "$work/metadata.json"

compared=$("$script" | sed -n 's/^each of the \([0-9]*\) requirements.*/\1/p')
if [ "${compared:-0}" -gt 0 ]; then
  echo "ok: this workspace compares $compared requirements"
else
  echo "FAILED: this workspace compares no requirement"
  failures=$((failures + 1))
fi

[ "$failures" = 0 ] || { echo "$failures case(s) failed" >&2; exit 1; }
