import { afterAll, describe, expect, it } from 'bun:test';
import { join } from 'node:path';
import { connectMCP, connectMCPServers, contentText, mcpToolName, type MCPConnection } from '../src/adapters/mcp.js';
import { mcpHttpHandler, sessions } from './fixtures/mcp-fake.js';

// The MCP adapter. The same cases run in the Go package (adapters/mcp).

const FAKE = join(import.meta.dir, 'fixtures', 'mcp-fake.ts');
const call = (c: MCPConnection, name: string, args: unknown) =>
  (c.tools[name] as any).execute(args, { toolCallId: 'c1', messages: [] });

describe('mcp over stdio', () => {
  it('lists every page of tools, names them with the prefix, and calls them', async () => {
    const c = await connectMCP({ name: 'fake', transport: { type: 'stdio', command: process.execPath, args: [FAKE] } });
    try {
      expect(Object.keys(c.tools).sort()).toEqual(['fake_add', 'fake_echo', 'fake_fail', 'fake_slow', 'fake_weird_name']);
      expect(await call(c, 'fake_echo', { text: 'hello' })).toBe('echo: hello');
      expect(await call(c, 'fake_fail', {})).toEqual({ error: 'it broke' });
      expect(await call(c, 'fake_add', { a: 2, b: 3 })).toBe('{"sum":5}');
      expect(await call(c, 'fake_weird_name', {})).toBe('hi\n[image image/png]');
    } finally { await c.close(); }
  });

  it('keeps only the tools asked for and marks the ones that need approval', async () => {
    const c = await connectMCP({
      name: 'fake', prefix: '', tools: ['echo', 'fail'], requiresConfirmation: ['fail'],
      transport: { type: 'stdio', command: process.execPath, args: [FAKE] },
    });
    try {
      expect(Object.keys(c.tools).sort()).toEqual(['echo', 'fail']);
      expect((c.tools.fail as any).requiresConfirmation).toBe(true);
      expect((c.tools.echo as any).requiresConfirmation).toBeUndefined();
    } finally { await c.close(); }
  });

  it('throws at connect for a tool the server does not have', async () => {
    await expect(connectMCP({ name: 'fake', tools: ['nope'], transport: { type: 'stdio', command: process.execPath, args: [FAKE] } }))
      .rejects.toThrow('no tool called "nope"');
  });

  it('gives the model an error when a call takes too long', async () => {
    const c = await connectMCP({ name: 'fake', timeoutMs: 300, transport: { type: 'stdio', command: process.execPath, args: [FAKE] } });
    try {
      expect(await call(c, 'fake_slow', {})).toEqual({ error: 'fake: fake: tools/call timed out after 300ms' });
      expect(await call(c, 'fake_echo', { text: 'still up' })).toBe('echo: still up');
    } finally { await c.close(); }
  });

  it('gives the model an error after the server is gone', async () => {
    const c = await connectMCP({ name: 'fake', transport: { type: 'stdio', command: process.execPath, args: [FAKE] } });
    await c.close();
    const out = await call(c, 'fake_echo', { text: 'x' });
    expect(out.error).toContain('the connection is closed');
  });

  it('throws at connect when the command cannot start', async () => {
    await expect(connectMCP({ name: 'nope', transport: { type: 'stdio', command: '/no/such/mcp-server' } })).rejects.toThrow();
  });
});

describe('mcp over http', () => {
  const server = Bun.serve({ port: 0, fetch: mcpHttpHandler });
  afterAll(() => server.stop(true));
  const url = `http://localhost:${server.port}/mcp`;

  it('keeps the session, reads event-stream answers, and ends the session on close', async () => {
    const before = { ...sessions };
    const c = await connectMCP({ name: 'web', transport: { type: 'http', url } });
    expect(Object.keys(c.tools)).toHaveLength(5);
    expect(await call(c, 'web_echo', { text: 'over http' })).toBe('echo: over http');
    expect(await call(c, 'web_fail', {})).toEqual({ error: 'it broke' });
    await c.close();
    expect(sessions.opened).toBe(before.opened + 1);
    expect(sessions.deleted).toBe(before.deleted + 1);
  });

  it('merges servers and refuses two tools with one name', async () => {
    const both = await connectMCPServers([
      { name: 'a', transport: { type: 'http', url } },
      { name: 'b', transport: { type: 'stdio', command: process.execPath, args: [FAKE] } },
    ]);
    expect(Object.keys(both.tools)).toHaveLength(10);
    await both.close();
    await expect(connectMCPServers([
      { name: 'a', prefix: '', transport: { type: 'http', url } },
      { name: 'b', prefix: '', transport: { type: 'stdio', command: process.execPath, args: [FAKE] } },
    ])).rejects.toThrow('two tools are called');
  });
});

describe('mcp helpers', () => {
  it('makes names providers accept', () => {
    expect(mcpToolName('gh_', 'repos.list/all')).toBe('gh_repos_list_all');
    expect(mcpToolName('', 'x'.repeat(80))).toHaveLength(64);
  });
  it('turns content into text', () => {
    expect(contentText({ content: [{ type: 'resource', resource: { uri: 'file:///a', text: 'body' } }] })).toBe('body');
    expect(contentText({ content: [{ type: 'audio', mimeType: 'audio/wav' }] })).toBe('[audio audio/wav]');
    expect(contentText({})).toBe('');
  });
});
