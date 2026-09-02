#!/usr/bin/env bash
# Pack each package and import it the way a consumer would.
#
# `tsc` passing says the source compiles; it says nothing about whether the
# published export map points at files the build actually emits. This installs
# the tarball into a throwaway project and imports every entry point.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

echo "--- building"
bun run --cwd "$root" build

cd "$work"
cat > package.json <<'JSON'
{ "name": "consumer-check", "private": true, "type": "module", "version": "1.0.0" }
JSON

echo "--- packing"
(cd "$root/packages/agentrun" && npm pack --silent --pack-destination "$work")
(cd "$root/packages/use-agentrun" && npm pack --silent --pack-destination "$work")

echo "--- installing"
bun add ./agentrun-*.tgz ./use-agentrun-*.tgz react

cat > check.mjs <<'JS'
const fail = (m) => { console.error('FAIL: ' + m); process.exit(1); };

// Every entry point in the export maps.
const core = await import('agentrun');
const mem = await import('agentrun/adapters/memory');
const adminMem = await import('agentrun/admin/memory');
const adminSqlite = await import('agentrun/admin/sqlite');
const react = await import('use-agentrun');

if (typeof core.setupAgentCore !== 'function') fail('core.setupAgentCore missing');
if (typeof mem.MemoryStorage !== 'function') fail('adapters/memory missing');
if (typeof adminMem.MemoryAdminStore !== 'function') fail('admin/memory missing');
if (!Object.keys(adminSqlite).length) fail('admin/sqlite empty');
if (typeof react.useAgentThread !== 'function') fail('use-agentrun hook missing');
if (typeof react.AgentKitProvider !== 'function') fail('AgentKitProvider missing');

// The hook package's routing is pure — exercise it without a DOM.
const url = react.routeUrl(react.defaultRoutes.stream, { threadId: 't1', since: 7 }, '');
if (url !== '/api/agent/stream?threadId=t1&since=7') fail('route resolution: ' + url);

console.log('ok: both packages import and resolve from a clean install');
JS

bun run check.mjs
