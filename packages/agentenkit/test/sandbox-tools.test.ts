import { describe, expect, it } from 'bun:test';
import { mkdtempSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { simulateReadableStream } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { LocalSandbox } from '../src/adapters/local-sandbox.js';
import { forgetSandboxHandles, sandboxKey } from '../src/core/builtin/sandbox.js';
import { cutMiddle } from '../src/core/builtin/sandbox-tools.js';
import { toolUseRow } from '../src/core/builtin/run.js';
import { resolveConfig, type AgentConfig } from '../src/core/types.js';
import type { BuiltinToolName, BuiltinToolOptions } from '../src/core/builtin/index.js';
import type { Pricer } from '../src/ports/runtime.js';
import * as pricing from '../src/pricing.js';
import { ofType, threadItems } from './stream-helpers.js';

// bash, code_execution and text_editor (spec T4). The same cases run in the
// Go package (sandbox_tools_test.go), under the same names.

interface Call {
  id: string;
  name: string;
  args: unknown;
}

/** A runtime whose model makes the calls of each step in turn (one step per
 *  model call, across runs), and answers "done" when the script runs out. */
async function harness(
  steps: Call[][],
  names: BuiltinToolName[],
  options: BuiltinToolOptions = {},
  opts: { config?: Partial<AgentConfig>; pricer?: Pricer } = {},
) {
  forgetSandboxHandles();
  let call = 0;
  const model = new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock',
    doStream: async () => {
      const calls = steps[call++] ?? [];
      const chunks: LanguageModelV1StreamPart[] = calls.length
        ? calls.map((c) => ({ type: 'tool-call', toolCallType: 'function', toolCallId: c.id, toolName: c.name, args: JSON.stringify(c.args) }))
        : [{ type: 'text-delta', textDelta: 'done' }];
      chunks.push({ type: 'finish', finishReason: calls.length ? 'tool-calls' : 'stop', usage: { promptTokens: 1, completionTokens: 1 } });
      return { stream: simulateReadableStream({ chunks }), rawCall: { rawPrompt: null, rawSettings: {} } };
    },
  });
  const storage = new MemoryStorage();
  const bus = new MemoryBus();
  const queue = new MemoryQueue();
  const kv = new MemoryKv();
  const runtime = await setupAgentCore({
    storage, bus, queue, kv,
    admin: new MemoryAdminStore(),
    tools: { sandbox: new LocalSandbox({ rootDir: mkdtempSync(join(tmpdir(), 'sandbox-tools-')) }) },
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
    ...(opts.pricer ? { pricer: opts.pricer } : {}),
    config: resolveConfig({ stopPollMs: 5, promptCaching: false, ...opts.config }),
  });
  const agent = runtime.createStreamTextAgent({ name: 'coder', tools: runtime.builtinTools(names, options) });
  const next = () => runtime.worker.handleJob(queue.items.shift()!);
  const run = async (threadId?: string) => {
    const r = await agent.run({ prompt: 'go', ...(threadId ? { threadId } : {}) });
    await next();
    return r.threadId;
  };
  const result = (threadId: string, id: string): any => {
    for (const msg of storage.messages.store.get(threadId) ?? []) {
      for (const part of Array.isArray(msg.content) ? (msg.content as any[]) : []) {
        if (part?.type === 'tool-result' && part.toolCallId === id) return part.result;
      }
    }
    return undefined;
  };
  const state = async (threadId: string) => (await storage.threads.get(threadId))!.state;
  return { runtime, storage, bus, kv, run, next, result, state };
}

const bash = (id: string, command: string, extra: Record<string, unknown> = {}): Call => ({ id, name: 'bash', args: { command, ...extra } });
const edit = (id: string, args: Record<string, unknown>): Call => ({ id, name: 'text_editor', args });
const noApproval = { bash: { approval: false }, textEditor: { approval: false } } as const;

describe('sandbox tools (T4)', () => {
  it('the sandbox tools need a sandbox', async () => {
    const runtime = await setupAgentCore({
      storage: new MemoryStorage(), bus: new MemoryBus(), queue: new MemoryQueue(), kv: new MemoryKv(),
      admin: new MemoryAdminStore(), resolveModel: () => ({ instance: () => null as any, contextWindow: 1 }),
    });
    for (const name of ['bash', 'code_execution', 'text_editor'] as const) {
      expect(() => runtime.builtinTools([name])).toThrow(`builtinTools: ${name} needs setupAgentCore({ tools: { sandbox } })`);
    }
  });

  it('bash asks for approval by default and runs once approved', async () => {
    const h = await harness([[bash('b1', 'echo hi')]], ['bash']);
    const threadId = await h.run();
    expect(await h.state(threadId)).toBe('WAITING_FOR_INPUT');
    await h.runtime.hitl.respond({ threadId, toolCallId: 'b1', approved: true });
    await h.next();
    expect(h.result(threadId, 'b1')).toEqual({ stdout: 'hi\n', stderr: '', exitCode: 0 });
    expect(await h.state(threadId)).toBe('COMPLETED');
  });

  it('a denied bash command does not run', async () => {
    const h = await harness([[bash('b1', 'echo x > made.txt')], [edit('e1', { command: 'view', path: 'made.txt' })]], ['bash', 'text_editor']);
    const threadId = await h.run();
    await h.runtime.hitl.respond({ threadId, toolCallId: 'b1', approved: false });
    await h.next();
    expect(h.result(threadId, 'b1')).toEqual({ denied: true });
    expect(h.result(threadId, 'e1')).toEqual({ error: 'text_editor: made.txt does not exist' });
  });

  it('an approval rule can decide per call', async () => {
    const h = await harness([[bash('b1', 'echo safe')], [bash('b2', 'rm -rf data')]], ['bash'], {
      bash: { approval: (input) => /\brm\b/.test(input.command ?? '') },
    });
    const threadId = await h.run();
    expect(h.result(threadId, 'b1')).toEqual({ stdout: 'safe\n', stderr: '', exitCode: 0 });
    expect(h.result(threadId, 'b2')).toBeUndefined();
    expect(await h.state(threadId)).toBe('WAITING_FOR_INPUT');
  });

  it('bash keeps the working folder between commands', async () => {
    const h = await harness(
      [[bash('b1', 'mkdir -p sub && cd sub')], [bash('b2', 'basename "$PWD"')], [bash('b3', 'basename "$PWD"', { restart: true })]],
      ['bash'],
      noApproval,
    );
    const threadId = await h.run();
    expect(h.result(threadId, 'b2').stdout).toBe('sub\n');
    expect(h.result(threadId, 'b3').stdout).toBe('work\n');
  });

  it('bash keeps the folder when the command sets its own exit trap, and keeps its exit code', async () => {
    const h = await harness(
      [[bash('b1', "mkdir -p sub && cd sub && trap 'true' EXIT")], [bash('b2', 'basename "$PWD"; false')]],
      ['bash'],
      noApproval,
    );
    const threadId = await h.run();
    expect(h.result(threadId, 'b2')).toEqual({ stdout: 'sub\n', stderr: '', exitCode: 1 });
  });

  it('bash output is cut in the middle past the result cap', async () => {
    const h = await harness([[bash('b1', "printf 'start'; head -c 500 /dev/zero | tr '\\0' x; printf 'end'")]], ['bash'], noApproval, {
      config: { builtinToolResultCapChars: 100 },
    });
    const threadId = await h.run();
    const r = h.result(threadId, 'b1');
    expect(r.truncated).toBe(true);
    expect(r.stdout.startsWith('start')).toBe(true);
    expect(r.stdout.endsWith('end')).toBe(true);
    expect(r.stdout).toContain('\n… 408 characters cut …\n');
  });

  it('bash reports a command stopped at its time limit', async () => {
    const h = await harness([[bash('b1', 'echo started; sleep 10')]], ['bash'], { bash: { approval: false, timeoutMs: 1_000 } });
    const threadId = await h.run();
    expect(h.result(threadId, 'b1')).toEqual({
      stdout: 'started\n', stderr: '', exitCode: 124, timedOut: true,
      error: 'bash: the command was stopped after 1 s',
    });
  });

  it('bash output goes out live as tool.output events', async () => {
    const h = await harness([[bash('b1', 'echo hello; echo oops >&2')]], ['bash'], noApproval);
    const threadId = await h.run();
    // Live only: on the run's stream as CUSTOM, never in the thread record.
    const custom = ofType(await threadItems(h.runtime.ports(), threadId), 'CUSTOM') as any[];
    const outputs = custom.filter((c) => c.name === 'tool.output').map((c) => c.value);
    expect(outputs.map((e) => [e.toolCallId, e.tool, e.stream, e.text]).sort()).toEqual([
      ['b1', 'bash', 'stderr', 'oops\n'],
      ['b1', 'bash', 'stdout', 'hello\n'],
    ]);
    expect((await h.runtime.events.since(threadId, -1)).some((e) => e.type === 'tool.output')).toBe(false);
  });

  it('code_execution runs Python without approval and lists the files it made', async () => {
    const code = "open('chart.png', 'wb').write(b'png')\nopen('data/out.csv', 'w').write('a,b')\nprint(1 + 1)";
    const h = await harness(
      [[bash('b0', 'mkdir -p data')], [{ id: 'c1', name: 'code_execution', args: { code } }]],
      ['bash', 'code_execution'],
      noApproval,
    );
    const threadId = await h.run();
    expect(h.result(threadId, 'c1')).toEqual({
      stdout: '2\n', stderr: '', exitCode: 0,
      files: [{ path: 'chart.png', mediaType: 'image/png' }, { path: 'data/out.csv', mediaType: 'text/csv' }],
    });
  });

  it('code_execution can import a module from the work folder', async () => {
    const h = await harness(
      [[bash('b0', "printf 'X = 41\\n' > helper.py")], [{ id: 'c1', name: 'code_execution', args: { code: 'import helper\nprint(helper.X + 1)' } }]],
      ['bash', 'code_execution'],
      noApproval,
    );
    const threadId = await h.run();
    expect(h.result(threadId, 'c1')).toEqual({ stdout: '42\n', stderr: '', exitCode: 0, files: [] });
  });

  it('code_execution reports a failing program', async () => {
    const h = await harness([[{ id: 'c1', name: 'code_execution', args: { code: "raise ValueError('boom')" } }]], ['code_execution']);
    const threadId = await h.run();
    const r = h.result(threadId, 'c1');
    expect(r.exitCode).toBe(1);
    expect(r.error).toBe('code_execution: the program exited with code 1');
    expect(r.stderr).toContain('ValueError: boom');
    expect(r.files).toEqual([]);
  });

  it('text_editor views without approval and asks before a change', async () => {
    const h = await harness([[edit('e1', { command: 'view', path: '.' })], [edit('e2', { command: 'create', path: 'a.txt', file_text: 'x' })]], ['text_editor']);
    const threadId = await h.run();
    expect(h.result(threadId, 'e1')).toEqual({ path: '.', content: '' });
    expect(await h.state(threadId)).toBe('WAITING_FOR_INPUT');
    await h.runtime.hitl.respond({ threadId, toolCallId: 'e2', approved: true });
    await h.next();
    expect(h.result(threadId, 'e2')).toEqual({ ok: true, message: 'Created a.txt.' });
  });

  it('text_editor creates, replaces, inserts and undoes', async () => {
    const h = await harness(
      [
        [edit('e1', { command: 'create', path: 'notes/a.txt', file_text: 'one\ntwo\nthree' })],
        [edit('e2', { command: 'str_replace', path: 'notes/a.txt', old_str: 'two', new_str: 'TWO' })],
        [edit('e3', { command: 'insert', path: 'notes/a.txt', insert_line: 1, new_str: 'one and a half' })],
        [edit('e4', { command: 'view', path: 'notes/a.txt' })],
        [edit('e5', { command: 'undo_edit', path: 'notes/a.txt' })],
        [edit('e6', { command: 'view', path: 'notes/a.txt', view_range: [2, -1] })],
        [edit('e7', { command: 'view', path: '.' })],
      ],
      ['text_editor'],
      noApproval,
    );
    const threadId = await h.run();
    expect(h.result(threadId, 'e1')).toEqual({ ok: true, message: 'Created notes/a.txt.' });
    expect(h.result(threadId, 'e2')).toEqual({ ok: true, message: 'Replaced 1 place in notes/a.txt.' });
    expect(h.result(threadId, 'e3')).toEqual({ ok: true, message: 'Inserted after line 1 of notes/a.txt.' });
    expect(h.result(threadId, 'e4')).toEqual({
      path: 'notes/a.txt', totalLines: 4,
      content: '     1\tone\n     2\tone and a half\n     3\tTWO\n     4\tthree',
    });
    expect(h.result(threadId, 'e5')).toEqual({ ok: true, message: 'Undid the last change to notes/a.txt.' });
    expect(h.result(threadId, 'e6')).toEqual({ path: 'notes/a.txt', totalLines: 3, content: '     2\tTWO\n     3\tthree' });
    // Hidden entries (the tools' own .agentenkit folder) are left out.
    expect(h.result(threadId, 'e7')).toEqual({ path: '.', content: 'notes/\nnotes/a.txt' });
  });

  it('str_replace needs exactly one match', async () => {
    const h = await harness(
      [
        [edit('e1', { command: 'create', path: 'a.txt', file_text: 'x x' })],
        [edit('e2', { command: 'str_replace', path: 'a.txt', old_str: 'y', new_str: 'z' })],
        [edit('e3', { command: 'str_replace', path: 'a.txt', old_str: 'x', new_str: 'z' })],
      ],
      ['text_editor'],
      noApproval,
    );
    const threadId = await h.run();
    expect(h.result(threadId, 'e2')).toEqual({ error: 'text_editor: old_str was not found in a.txt; it must match exactly, whitespace included' });
    expect(h.result(threadId, 'e3')).toEqual({ error: 'text_editor: old_str matches 2 places in a.txt; include more of the text around it so it matches one' });
  });

  it('each sandbox tool call books a usage row with its seconds', async () => {
    const h = await harness([[bash('b1', 'sleep 0.2')]], ['bash'], noApproval, { pricer: pricing.tools({ local: { perSecond: 1 } }) });
    await h.run();
    const rows = h.storage.usage.recorded.filter((u) => u.kind === 'tool');
    expect(rows).toHaveLength(1);
    expect(rows[0]!.model).toBe('tool:bash');
    expect(rows[0]!.modelId).toBe('local');
    const seconds = (rows[0]!.providerMetadata as any).seconds as number;
    expect(seconds).toBeGreaterThanOrEqual(0.2);
    expect(rows[0]!.cost?.micros).toBe(Math.round(seconds * 1_000_000));
  });

  it('a call on a fresh sandbox after the old one ended says the files are gone', async () => {
    const h = await harness(
      [[edit('e1', { command: 'create', path: 'a.txt', file_text: 'x' })], [], [edit('e2', { command: 'view', path: 'a.txt' })]],
      ['text_editor'],
      noApproval,
    );
    const threadId = await h.run();
    const rec = JSON.parse((await h.kv.get(sandboxKey(threadId)))!);
    await h.kv.set(sandboxKey(threadId), JSON.stringify({ ...rec, lastUsedAt: 0 }));
    await h.run(threadId);
    expect(h.result(threadId, 'e2')).toEqual({
      error: 'text_editor: a.txt does not exist',
      note: 'The sandbox this conversation used before has ended, so files from earlier are gone. This is a fresh one.',
    });
  });

  it('the sandbox tools stop at maxUses', async () => {
    const h = await harness([[bash('b1', 'true')], [bash('b2', 'true')]], ['bash'], { bash: { approval: false, maxUses: 1 } });
    const threadId = await h.run();
    expect(h.result(threadId, 'b1').exitCode).toBe(0);
    expect(h.result(threadId, 'b2')).toEqual({ error: 'bash: this run has used its 1 commands' });
  });

  it('tool prices can be per second, per use or both', () => {
    const p = pricing.tools({ e2b: { perSecond: 0.0001 }, docker: { perUse: 0.001, perSecond: 0.0001 } });
    const price = (adapter: string, seconds: number) =>
      (p.price({ ...toolUseRow('bash', adapter, 1, seconds), runId: 'r', agentId: null }) as { micros: number }).micros;
    expect(price('e2b', 2.5)).toBe(250);
    expect(price('docker', 2.5)).toBe(1_250);
    expect(price('e2b', 0)).toBe(0);
  });

  it('cutMiddle keeps the start and the end and never splits a character', () => {
    expect(cutMiddle('abcdefghij', 20)).toEqual({ text: 'abcdefghij', cut: false });
    expect(cutMiddle('abcdefghij', 4)).toEqual({ text: 'ab\n… 6 characters cut …\nij', cut: true });
    // 😀 is two UTF-16 units; neither half is kept alone.
    expect(cutMiddle('a😀bcdef😀g', 4).text).toBe('a\n… 9 characters cut …\ng');
  });
});
