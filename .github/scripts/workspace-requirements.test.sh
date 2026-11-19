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

wasmtime() {
  local version="$1" features="$2"
  mkdir -p "$work/wasmtime/src"
  printf '[package]\nname = "wasmtime"\nversion = "%s"\nedition = "2021"\n\n[features]\ngc = []\nthreads = []\n' \
    "$version" >"$work/wasmtime/Cargo.toml"
  touch "$work/wasmtime/src/lib.rs"
  workspace "$inherits" dependencies "cufflink-types = { workspace = true }
wasmtime = { path = \"../../wasmtime\", version = \"$version\", features = [$features] }"
}

audit() {
  mkdir -p "$work/workspace/.cargo"
  printf '%s\n' "$@" >"$work/workspace/.cargo/audit.toml"
}

ignored() {
  local revisit="$1"
  audit '[advisories]' 'ignore = [' \
    "    # revisit $revisit: GHSA-2674-w29h-wqvj affects wasmtime >=39.0.0" \
    '    "RUSTSEC-2026-0567",' \
    "    # revisit $revisit: GHSA-wvgq-g5hf-6gm6 affects wasmtime >=47.0.0" \
    '    "RUSTSEC-2026-0573",' \
    ']'
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
  "cufflink-cli requires cufflink-types ^0.23.0 from registry+https://github.com/rust-lang/crates.io-index, not the cufflink-types 0.23.0 the workspace builds" \
  "$work/metadata.json"

workspace "" dependencies 'cufflink-types = { git = "https://git.example.com/cufflink.git" }'
expect "a workspace crate required from git" 1 \
  "cufflink-cli requires cufflink-types * from git+https://git.example.com/cufflink.git, not the cufflink-types 0.23.0 the workspace builds" \
  "$work/metadata.json"

workspace "" dependencies 'cufflink-types = { path = "../types" }'
expect "nothing to compare" 1 "no requirement on a workspace crate was found" \
  "$work/metadata.json"

wasmtime 36.0.21 ""
ignored 2999-12-31
expect "the audit exceptions on wasmtime 36 without gc or threads" 0 \
  "the audit ignores RUSTSEC-2026-0567, RUSTSEC-2026-0573 hold for wasmtime 36.0.21 without gc or threads until 2999-12-31" \
  "$work/metadata.json"

wasmtime 48.0.9 '"gc", "threads"'
expect "another wasmtime major and features with no audit exceptions" 0 "each of the 1 requirements" \
  "$work/metadata.json"

wasmtime 48.0.9 ""
ignored 2999-12-31
expect "the audit exceptions on another wasmtime major" 1 \
  "the audit exceptions hold for wasmtime 36 only, and the workspace builds wasmtime 48.0.9: drop them" \
  "$work/metadata.json"

wasmtime 36.0.21 '"gc"'
ignored 2999-12-31
expect "the audit exceptions with the gc feature" 1 \
  "the audit exceptions hold only while wasmtime builds without its gc feature, and the workspace enables it" \
  "$work/metadata.json"

wasmtime 36.0.21 '"threads"'
ignored 2999-12-31
expect "the audit exceptions with the threads feature" 1 \
  "the audit exceptions hold only while wasmtime builds without its threads feature, and the workspace enables it" \
  "$work/metadata.json"

workspace "$inherits" dependencies 'cufflink-types = { workspace = true }'
ignored 2999-12-31
expect "the audit exceptions with no wasmtime" 1 \
  "the audit exceptions are for wasmtime, which the workspace no longer builds: drop them" \
  "$work/metadata.json"

wasmtime 36.0.21 ""
ignored 2000-01-01
expect "audit exceptions past their revisit date" 1 \
  "RUSTSEC-2026-0567 was to be revisited by 2000-01-01: check it still does not apply, then set a new date or drop the ignore" \
  "$work/metadata.json"

wasmtime 36.0.21 ""
ignored 2026-02-30
expect "audit exceptions with an impossible revisit date" 1 \
  "RUSTSEC-2026-0573 is to be revisited on 2026-02-30, which is not a date" "$work/metadata.json"

wasmtime 36.0.21 ""
audit '[advisories]' 'ignore = [' '    # GHSA-2674-w29h-wqvj affects wasmtime >=39.0.0' \
  '    "RUSTSEC-2026-0567",' ']'
expect "an audit exception with no revisit date" 1 \
  'each id in .cargo/audit.toml needs a "# revisit YYYY-MM-DD: <why>" line above it' \
  "$work/metadata.json"

wasmtime 36.0.21 ""
audit '[advisories]' 'ignore = [' '    "RUSTSEC-2026-0567",' ']'
expect "an audit exception with no note" 1 \
  ".cargo/audit.toml may hold only an [advisories] ignore list, each id under its revisit note" \
  "$work/metadata.json"

wasmtime 36.0.21 ""
audit '[advisories]' 'ignore = [' '    # revisit 2999-12-31: unmaintained' '    "RUSTSEC-2024-0436",' ']'
expect "an audit exception for another advisory" 1 \
  "RUSTSEC-2024-0436 is ignored, and only RUSTSEC-2026-0567 and RUSTSEC-2026-0573 may be" \
  "$work/metadata.json"

wasmtime 36.0.21 ""
audit '[advisories]' 'ignore = ["RUSTSEC-2024-0436"]'
expect "an inline audit ignore list" 1 \
  ".cargo/audit.toml may hold only an [advisories] ignore list, each id under its revisit note" \
  "$work/metadata.json"

wasmtime 36.0.21 ""
audit '[advisories]' 'ignore = [' ']' '[output]' 'deny = []'
expect "audit settings beyond the ignore list" 1 \
  ".cargo/audit.toml may hold only an [advisories] ignore list, each id under its revisit note" \
  "$work/metadata.json"

got=0
output=$("$script" 2>&1) || got=$?
compared=$(sed -n 's/^each of the \([0-9]*\) requirements.*/\1/p' <<<"$output")
if [ "$got" = 0 ] && [ "${compared:-0}" -gt 0 ]; then
  echo "ok: this workspace compares $compared requirements"
else
  printf 'FAILED: this workspace (exit %s, %s requirements compared)\n%s\n' \
    "$got" "${compared:-no}" "$output"
  failures=$((failures + 1))
fi

[ "$failures" = 0 ] || { echo "$failures case(s) failed" >&2; exit 1; }
