#!/usr/bin/env bash
# Publish the nested module at the root release commit, without moving tags.
set -euo pipefail

release="${1:?usage: publish-go-tag.sh VERSION COMMIT}"
commit="$(git rev-parse "${2:?release commit required}^{commit}")"
module_dir=packages/go-agentenkit
module_path=github.com/eadwinCode/agentic-kit/packages/go-agentenkit

if [[ ! "$release" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
  echo "Invalid release version: $release" >&2
  exit 1
fi

declared_module="$(git show "$commit:$module_dir/go.mod" | awk '$1 == "module" { print $2 }')"
if [[ "$declared_module" != "$module_path" ]]; then
  echo "Unexpected Go module: $declared_module" >&2
  exit 1
fi

# Prefer the peeled commit for annotated tags, otherwise the lightweight ref.
remote_commit() {
  git ls-remote --tags origin "refs/tags/$1" "refs/tags/$1^{}" |
    awk '/\^\{\}$/ { peeled = $1; next } { ref = $1 } END { print peeled ? peeled : ref }'
}

if [[ "$(remote_commit "$release")" != "$commit" ]]; then
  echo "Root release tag $release does not point to $commit; refusing publication" >&2
  exit 1
fi

module_tag="$module_dir/$release"
existing="$(remote_commit "$module_tag")"
if [[ -n "$existing" ]]; then
  if [[ "$existing" != "$commit" ]]; then
    echo "$module_tag already points to $existing; refusing to replace it" >&2
    exit 1
  fi
  echo "$module_tag already published at $commit"
  exit 0
fi

# No force: a concurrent publisher cannot overwrite an existing remote tag.
git push origin "$commit:refs/tags/$module_tag"
echo "Published $module_tag at $commit"
