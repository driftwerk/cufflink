#!/usr/bin/env bash
# Vendors the swagger-ui-dist files the public documents serve.
# Usage: vendor.sh <version> <sha512 integrity>
set -euo pipefail

version=${1:?usage: vendor.sh <version> <sha512 integrity>}
integrity=${2:?usage: vendor.sh <version> <sha512 integrity>}
files=(
  index.html
  swagger-ui.css
  index.css
  swagger-ui-bundle.js
  swagger-ui-standalone-preset.js
  favicon-32x32.png
  favicon-16x16.png
  LICENSE
  NOTICE
)

here=$(cd "$(dirname "$0")" && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

curl --fail --silent --show-error --location \
  --output "$work/package.tgz" \
  "https://registry.npmjs.org/swagger-ui-dist/-/swagger-ui-dist-$version.tgz"

actual="sha512-$(openssl dgst -sha512 -binary "$work/package.tgz" | openssl base64 -A)"
if [ "$actual" != "$integrity" ]; then
  echo "swagger-ui-dist $version is $actual, not $integrity; nothing vendored" >&2
  exit 1
fi

tar -xzf "$work/package.tgz" -C "$work" "${files[@]/#/package/}"
for file in "${files[@]}"; do
  cp "$work/package/$file" "$here/$file"
done
