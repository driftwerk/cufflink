#!/usr/bin/env bash
set -euo pipefail

metadata=$(mktemp)
resolved=$(mktemp)
trap 'rm -f "$metadata" "$resolved"' EXIT
if [ -n "${1:-}" ]; then
  cat "$1" >"$metadata"
else
  cargo metadata --no-deps --format-version 1 >"$metadata"
fi

checked=$(jq -c '
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
    }' "$metadata")

root=$(jq -r '.workspace_root' "$metadata")
config="$root/.cargo/audit.toml"
exceptions='{"ignored": [], "refused": []}'
if [ -f "$config" ]; then
  cargo metadata --format-version 1 --manifest-path "$root/Cargo.toml" >"$resolved"
  exceptions=$(jq -n -c --rawfile config "$config" --slurpfile metadata "$resolved" \
    --arg today "$(date -u +%F)" '
    def held: ["RUSTSEC-2026-0567", "RUSTSEC-2026-0573"];
    def real_date: (try (strptime("%Y-%m-%d") | mktime | strftime("%Y-%m-%d")) catch null) == .;
    $metadata[0] as $m
    | [$config | split("\n")[] | sub("\\s+$"; "") | select(length > 0)] as $lines
    | if ($lines | length) < 3 or ($lines | length) % 2 == 0 or $lines[0] != "[advisories]"
        or $lines[1] != "ignore = [" or $lines[-1] != "]"
      then {ignored: [], refused: [".cargo/audit.toml may hold only an [advisories] ignore list, each id under its revisit note"]}
      else
        [range(2; ($lines | length) - 1; 2) as $i
         | {note: $lines[$i], line: $lines[$i + 1]}
         | . + ((.note | [capture("^ +# revisit (?<revisit>[0-9]{4}-[0-9]{2}-[0-9]{2}): \\S.*$")][0]) // {})
         | . + ((.line | [capture("^ +\"(?<advisory>[A-Z]+-[0-9]{4}-[0-9]{4})\",?$")][0]) // {})] as $entries
        | [$m.packages[] | select(.name == "wasmtime")] as $wasmtime
        | ([$m.resolve.nodes[] | select(.id | IN($wasmtime[].id)) | .features[]
            | select(IN("gc", "threads"))] | unique) as $enabled
        | {
            ignored: [$entries[].advisory // empty],
            revisit: ([$entries[].revisit // empty] | min),
            wasmtime: ([$wasmtime[].version] | join(", ")),
            refused: (
              [$entries[]
               | if .advisory == null or .revisit == null then
                   "each id in .cargo/audit.toml needs a \"# revisit YYYY-MM-DD: <why>\" line above it, not \(.note) then \(.line)"
                 elif (.advisory | IN(held[]) | not) then
                   "\(.advisory) is ignored, and only \(held | join(" and ")) may be"
                 elif (.revisit | real_date | not) then
                   "\(.advisory) is to be revisited on \(.revisit), which is not a date"
                 elif .revisit < $today then
                   "\(.advisory) was to be revisited by \(.revisit): check it still does not apply, then set a new date or drop the ignore"
                 else empty end]
              + if ($entries | length) == 0 then []
                elif ($wasmtime | length) == 0 then
                  ["the audit exceptions are for wasmtime, which the workspace no longer builds: drop them"]
                else
                  [$wasmtime[] | select(.version | split(".")[0] != "36")
                   | "the audit exceptions hold for wasmtime 36 only, and the workspace builds wasmtime \(.version): drop them"]
                  + [$enabled[]
                     | "the audit exceptions hold only while wasmtime builds without its \(.) feature, and the workspace enables it"]
                end)
          }
      end')
fi

compared=$(jq -r '.compared' <<<"$checked")
if [ "$compared" = 0 ]; then
  echo "::error::no requirement on a workspace crate was found to compare"
  exit 1
fi
jq -r '.refused[] | "::error::\(.)"' <<<"$checked"
jq -r '.refused[] | "::error::\(.)"' <<<"$exceptions"
if jq -se 'any(.[]; .refused | length > 0)' <<<"$checked $exceptions" >/dev/null; then
  exit 1
fi
echo "each of the $compared requirements on a workspace crate names the version the workspace builds"
jq -r 'select(.ignored | length > 0)
  | "the audit ignores \(.ignored | join(", ")) hold for wasmtime \(.wasmtime) without gc or threads until \(.revisit)"' \
  <<<"$exceptions"
