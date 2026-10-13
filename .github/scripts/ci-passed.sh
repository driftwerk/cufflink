#!/usr/bin/env bash
set -euo pipefail

commit="${1:?usage: ci-passed.sh <commit>}"
repo="${GITHUB_REPOSITORY:?GITHUB_REPOSITORY must name the repository}"
release_only='^\.github/(workflows/publish\.yml|scripts/ci-passed\.sh)$'

green() {
  gh api "repos/$repo/actions/workflows/ci.yml/runs?head_sha=$1&status=success&per_page=1" --jq '.total_count'
}

for candidate in $(git rev-list --first-parent --max-count=20 "$commit"); do
  if [ "$(green "$candidate")" -gt 0 ]; then
    changed=$(git diff --name-only "$candidate" "$commit" | grep -Ev "$release_only" || true)
    if [ -z "$changed" ]; then
      echo "CI passed on $candidate"
      exit 0
    fi
    echo "::error::$commit changes files CI has not passed since $candidate:"
    echo "$changed"
    exit 1
  fi
done
echo "::error::no passing CI run on $commit or its recent ancestors"
exit 1
