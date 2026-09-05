#!/usr/bin/env bash
# Exercise publication against an isolated local remote; no GitHub access.
set -euo pipefail

script="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/publish-go-tag.sh"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
git init --bare -q "$work/remote.git"
git init -q "$work/repo"
cd "$work/repo"
git config user.name 'Release test'
git config user.email 'release-test@example.invalid'
git config commit.gpgsign false
git config tag.gpgsign false
git remote add origin "$work/remote.git"
mkdir -p packages/go-agentenkit
printf 'module github.com/eadwinCode/agentic-kit/packages/go-agentenkit\n' > packages/go-agentenkit/go.mod
git add .
git commit -qm 'Release fixture'
first="$(git rev-parse HEAD)"
git commit -qm 'Next release fixture' --allow-empty
second="$(git rev-parse HEAD)"

expect_failure() {
  if bash "$script" "$@" > "$work/failure.log" 2>&1; then
    echo "Expected publication to fail: $*" >&2
    exit 1
  fi
}

# A lightweight root produces the missing nested tag at the requested commit.
git tag v0.2.2 "$first"
git push -q origin refs/tags/v0.2.2
bash "$script" v0.2.2 "$first"
test "$(git --git-dir="$work/remote.git" rev-parse packages/go-agentenkit/v0.2.2)" = "$first"
# Re-running an already published release succeeds without changing its tag.
bash "$script" v0.2.2 "$first"

# Existing annotated tags are compared by their commit and left intact.
git tag -a v0.2.3 "$second" -m 'Root release'
git tag -a packages/go-agentenkit/v0.2.3 "$second" -m 'Module release'
git push -q origin refs/tags/v0.2.3 refs/tags/packages/go-agentenkit/v0.2.3
annotated="$(git rev-parse packages/go-agentenkit/v0.2.3)"
bash "$script" v0.2.3 "$second"
test "$(git --git-dir="$work/remote.git" rev-parse packages/go-agentenkit/v0.2.3)" = "$annotated"

# A conflicting module tag must never be replaced.
git tag v0.2.4 "$second"
git tag packages/go-agentenkit/v0.2.4 "$first"
git push -q origin refs/tags/v0.2.4 refs/tags/packages/go-agentenkit/v0.2.4
expect_failure v0.2.4 "$second"
test "$(git --git-dir="$work/remote.git" rev-parse packages/go-agentenkit/v0.2.4)" = "$first"

# A stale workflow cannot publish a different commit from the remote root tag.
git tag v0.2.5 "$second"
git push -q origin refs/tags/v0.2.5
expect_failure v0.2.5 "$first"
test -z "$(git ls-remote origin refs/tags/packages/go-agentenkit/v0.2.5)"
expect_failure v0.2.6 "$second" # Missing root tag.
expect_failure invalid "$second"

# Check the declaration in the release commit, not the caller's working tree.
printf 'module github.com/example/wrong-module\n' > packages/go-agentenkit/go.mod
git add .
git commit -qm 'Wrong module fixture'
git tag v0.2.7
git push -q origin refs/tags/v0.2.7
expect_failure v0.2.7 HEAD
test -z "$(git ls-remote origin refs/tags/packages/go-agentenkit/v0.2.7)"
bash "$script" v0.2.2 "$first"

echo 'ok: Go release publication, retries, annotated tags, and refusal checks'
