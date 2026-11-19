#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/../.."
script="$PWD/.github/scripts/workspace-requirements.sh"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
today=$(date -u +%F)
export WORKSPACE_REQUIREMENTS_TODAY="$today"

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
  local version="$1" features="$2" more="${3:-}"
  mkdir -p "$work/wasmtime/src" "$work/gc-helper/src"
  printf '[package]\nname = "wasmtime"\nversion = "%s"\nedition = "2021"\n\n[features]\ngc = []\nthreads = []\n' \
    "$version" >"$work/wasmtime/Cargo.toml"
  printf '[package]\nname = "gc-helper"\nversion = "0.1.0"\nedition = "2021"\n\n[dependencies]\nwasmtime = { path = "../wasmtime", features = ["gc"] }\n' \
    >"$work/gc-helper/Cargo.toml"
  touch "$work/wasmtime/src/lib.rs" "$work/gc-helper/src/lib.rs"
  workspace "$inherits" dependencies "cufflink-types = { workspace = true }
wasmtime = { path = \"../../wasmtime\", version = \"$version\", features = [$features] }
$more"
}

day() {
  jq -nr --arg today "$today" --argjson offset "$1" \
    '$today | strptime("%Y-%m-%d") | mktime + $offset * 86400 | strftime("%Y-%m-%d")'
}

audit() {
  mkdir -p "$work/workspace/.cargo"
  printf '%s\n' "$@" >"$work/workspace/.cargo/audit.toml"
  cargo generate-lockfile --offline --quiet --manifest-path "$work/workspace/Cargo.toml"
}

ignored() {
  local revisit="$1"
  audit '[advisories]' 'ignore = [' \
    "    # revisit $revisit: GHSA-2674-w29h-wqvj affects wasmtime >=39.0.0 <48.0.9" \
    '    "RUSTSEC-2026-0567",' \
    "    # revisit $revisit: GHSA-wvgq-g5hf-6gm6 affects wasmtime >=47.0.0 <=48.0.8" \
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

quiet() {
  local case="$1" output got=0
  shift
  output=$("$script" "$@" 2>&1) || got=$?
  if [ "$got" = 0 ] && ! grep -qF -- "::warning::" <<<"$output"; then
    echo "ok: $case"
  else
    printf 'FAILED: %s (exit %s, expected 0 with no warning)\n%s\n' "$case" "$got" "$output"
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
ignored "$(day 30)"
expect "the audit exceptions on wasmtime 36 without gc or threads" 0 \
  "the audit ignores RUSTSEC-2026-0567, RUSTSEC-2026-0573 hold for wasmtime 36.0.21 without gc or threads until $(day 30)" \
  "$work/metadata.json"
quiet "audit exceptions more than 14 days from their revisit date warn of nothing" "$work/metadata.json"

wasmtime 48.0.9 '"gc", "threads"'
expect "another wasmtime major and features with no audit exceptions" 0 "each of the 1 requirements" \
  "$work/metadata.json"

wasmtime 48.0.9 ""
ignored "$(day 30)"
expect "the audit exceptions on another wasmtime major" 1 \
  "the audit exceptions hold for wasmtime 36 only, and the workspace builds wasmtime 48.0.9: drop them" \
  "$work/metadata.json"

wasmtime 36.0.21 '"gc"'
ignored "$(day 30)"
expect "the audit exceptions with the gc feature" 1 \
  "the audit exceptions hold only while wasmtime builds without its gc feature, and the workspace enables it" \
  "$work/metadata.json"

wasmtime 36.0.21 '"threads"'
ignored "$(day 30)"
expect "the audit exceptions with the threads feature" 1 \
  "the audit exceptions hold only while wasmtime builds without its threads feature, and the workspace enables it" \
  "$work/metadata.json"

workspace "$inherits" dependencies 'cufflink-types = { workspace = true }'
ignored "$(day 30)"
expect "the audit exceptions with no wasmtime" 1 \
  "the audit exceptions are for wasmtime, which the workspace no longer builds: drop them" \
  "$work/metadata.json"

wasmtime 36.0.21 "" '[features]
wasm-gc = ["wasmtime/gc"]'
ignored "$(day 30)"
expect "the audit exceptions with gc behind a feature" 1 \
  "the audit exceptions hold only while wasmtime builds without its gc feature, and the workspace enables it" \
  "$work/metadata.json"

wasmtime 36.0.21 "" 'gc-helper = { path = "../../gc-helper", optional = true }'
ignored "$(day 30)"
expect "the audit exceptions with gc behind an optional dependency" 1 \
  "the audit exceptions hold only while wasmtime builds without its gc feature, and the workspace enables it" \
  "$work/metadata.json"

wasmtime 36.0.21 ""
ignored "$(day -1)"
expect "audit exceptions the day after their revisit date" 1 \
  "RUSTSEC-2026-0567 was to be revisited by $(day -1): check it still does not apply, then set a new date or drop the ignore" \
  "$work/metadata.json"

ignored "$(day 0)"
expect "audit exceptions on their revisit date" 0 \
  "::warning::RUSTSEC-2026-0567 is to be revisited by $(day 0), in 0 days" "$work/metadata.json"

ignored "$(day 14)"
expect "audit exceptions 14 days from their revisit date" 0 \
  "::warning::RUSTSEC-2026-0573 is to be revisited by $(day 14), in 14 days: check it still does not apply, then set a new date or drop the ignore" \
  "$work/metadata.json"

ignored "$(day 15)"
quiet "audit exceptions 15 days from their revisit date" "$work/metadata.json"

ignored "$(day 14)"
expect "audit exceptions 14 days from their revisit date, failing within 14" 1 \
  "::error::RUSTSEC-2026-0567 is to be revisited by $(day 14), in 14 days" \
  "$work/metadata.json" --fail-within 14

ignored "$(day 15)"
quiet "audit exceptions 15 days from their revisit date, failing within 14" \
  "$work/metadata.json" --fail-within 14

expect "a failing window that is not a number of days" 1 \
  "--fail-within takes a number of days from 0 to 60, not soon" "$work/metadata.json" --fail-within soon

expect "a failing window past 60 days" 1 \
  "--fail-within takes a number of days from 0 to 60, not 61" "$work/metadata.json" --fail-within 61

expect "a failing window that would overflow" 1 \
  "--fail-within takes a number of days from 0 to 60, not 9223372036854775808" \
  "$work/metadata.json" --fail-within 9223372036854775808

ignored "$(day 30)"
WORKSPACE_REQUIREMENTS_TODAY=2026-1-5 expect "a today that is not a YYYY-MM-DD date" 1 \
  "today is 2026-1-5, which is not a YYYY-MM-DD date" "$work/metadata.json"

audit '[advisories]' 'ignore = [' \
  "    # revisit $(day 30): GHSA-2674-w29h-wqvj affects wasmtime >=39.0.0 <48.0.9" '    "RUSTSEC-2026-0567",' \
  "    # revisit $(day 30): GHSA-2674-w29h-wqvj affects wasmtime >=39.0.0 <48.0.9" '    "RUSTSEC-2026-0567",' ']'
expect "an advisory ignored twice" 1 "RUSTSEC-2026-0567 is listed more than once" "$work/metadata.json"

ignored "$(day 30)"
printf 'gc-helper = { path = "../../gc-helper" }\n' >>"$work/workspace/cli/Cargo.toml"
expect "audit exceptions against a stale lock" 101 "--locked" "$work/metadata.json"

wasmtime 36.0.21 ""
ignored "$(day 60)"
expect "audit exceptions revisited 60 days out" 0 "the audit ignores RUSTSEC-2026-0567, RUSTSEC-2026-0573" \
  "$work/metadata.json"

ignored "$(day 61)"
expect "audit exceptions revisited more than 60 days out" 1 \
  "RUSTSEC-2026-0567 is to be revisited on $(day 61), more than 60 days after $(day 0)" \
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
audit '[advisories]' 'ignore = [' "    # revisit $(day 30): unmaintained" '    "RUSTSEC-2024-0436",' ']'
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
output=$(WORKSPACE_REQUIREMENTS_TODAY=2000-01-01 "$script" 2>&1) || got=$?
if [ "$got" = 0 ]; then
  echo "ok: this workspace ignores the test-only today"
else
  printf 'FAILED: this workspace read the test-only today (exit %s)\n%s\n' "$got" "$output"
  failures=$((failures + 1))
fi

got=0
output=$(env -u WORKSPACE_REQUIREMENTS_TODAY "$script" 2>&1) || got=$?
compared=$(sed -n 's/^each of the \([0-9]*\) requirements.*/\1/p' <<<"$output")
if [ "$got" = 0 ] && [ "${compared:-0}" -gt 0 ]; then
  echo "ok: this workspace compares $compared requirements"
else
  printf 'FAILED: this workspace (exit %s, %s requirements compared)\n%s\n' \
    "$got" "${compared:-no}" "$output"
  failures=$((failures + 1))
fi

[ "$failures" = 0 ] || { echo "$failures case(s) failed" >&2; exit 1; }
