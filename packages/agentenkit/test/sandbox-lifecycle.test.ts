import { describe, expect, it } from 'bun:test';
import { simulateReadableStream, tool } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { z } from 'zod';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemorySandbox, MemoryStorage } from '../src/adapters/memory.js';
import { forgetSandboxHandles, sandboxKey, threadSandbox, withSandbox } from '../src/core/builtin/sandbox.js';
import { resolveConfig } from '../src/core/types.js';

// One sandbox per thread (spec T3). The same cases run in the Go package
// (sandbox_lifecycle_test.go), under the same names.

const IDLE = 30 * 60_000;
const LIFETIME = 24 * 60 * 60_000;

interface Seen {
  sandboxId: string;
  created: boolean;
  lost: boolean;
  files: string[];
}

/** A runtime whose agent calls `touch` `calls` times at once, then answers.
 *  `touch` writes a file in the thread's sandbox and says what it found. */
async function harness(calls = 1) {
  const sandboxes = new MemorySandbox();
  const kv = new MemoryKv();
  const queue = new MemoryQueue();
  const seen: Seen[] = [];
  const model = new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock',
    doStream: async ({ prompt }: any) => {
      const answered = prompt.at(-1)?.role === 'tool';
      const chunks: LanguageModelV1StreamPart[] = [];
      if (answered) chunks.push({ type: 'text-delta', textDelta: 'done' });
      else {
        for (let i = 0; i < calls; i++) {
          chunks.push({ type: 'tool-call', toolCallType: 'function', toolCallId: `c${i}`, toolName: 'touch', args: JSON.stringify({ name: `f${i}` }) });
        }
      }
      chunks.push({ type: 'finish', finishReason: answered ? 'stop' : 'tool-calls', usage: { promptTokens: 1, completionTokens: 1 } });
      return { stream: simulateReadableStream({ chunks }), rawCall: { rawPrompt: null, rawSettings: {} } };
    },
  });
  const runtime = await setupAgentCore({
    storage: new MemoryStorage(), bus: new MemoryBus(), queue, kv,
    admin: new MemoryAdminStore(),
    tools: { sandbox: sandboxes },
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
    config: resolveConfig({ stopPollMs: 5, promptCaching: false }),
  });
  const touch = tool({
    description: 'Write a file in the sandbox',
    parameters: z.object({ name: z.string() }),
    execute: (args, opts) =>
      withSandbox(opts, async ({ sandbox, created, lost }) => {
        await sandbox.filesystem.writeFile(`${args.name}.txt`, 'x');
        const files = (await sandbox.filesystem.readdir('.')).map((e) => e.name);
        seen.push({ sandboxId: sandbox.sandboxId, created, lost, files });
        return 'ok';
      }),
  });
  const agent = runtime.createStreamTextAgent({ name: 'coder', tools: { touch } });
  const run = async (threadId?: string) => {
    const r = await agent.run({ prompt: 'go', ...(threadId ? { threadId } : {}) });
    await runtime.worker.handleJob(queue.items.shift()!);
    return r.threadId;
  };
  const record = async (threadId: string) => JSON.parse((await kv.get(sandboxKey(threadId)))!);
  const setRecord = async (threadId: string, change: Record<string, number>) =>
    kv.set(sandboxKey(threadId), JSON.stringify({ ...(await record(threadId)), ...change }));
  return { runtime, sandboxes, kv, seen, run, record, setRecord };
}

describe('one sandbox per thread', () => {
  it('the first sandbox call in a thread makes one', async () => {
    const h = await harness();
    const threadId = await h.run();
    expect(h.sandboxes.created).toHaveLength(1);
    expect(h.sandboxes.created[0]!.metadata.threadId).toBe(threadId);
    expect(h.sandboxes.created[0]!.timeoutMs).toBe(IDLE + 60_000);
    expect(h.seen).toEqual([{ sandboxId: 'mem-1', created: true, lost: false, files: ['f0.txt'] }]);
    expect((await h.record(threadId)).id).toBe('mem-1');
  });

  it('a later run in the thread finds the same sandbox and its files', async () => {
    const h = await harness();
    const threadId = await h.run();
    forgetSandboxHandles(); // as another worker would come to it
    await h.run(threadId);
    expect(h.sandboxes.created).toHaveLength(1);
    expect(h.seen[1]).toEqual({ sandboxId: 'mem-1', created: false, lost: false, files: ['f0.txt'] });
  });

  it('parallel calls in one thread share one sandbox', async () => {
    const h = await harness(3);
    await h.run();
    expect(h.sandboxes.created).toHaveLength(1);
    expect(new Set(h.seen.map((s) => s.sandboxId))).toEqual(new Set(['mem-1']));
  });

  it('a sandbox idle past sandboxIdleTtlMs is replaced and its files are lost', async () => {
    const h = await harness();
    const threadId = await h.run();
    await h.setRecord(threadId, { lastUsedAt: Date.now() - IDLE - 1_000 });
    await h.run(threadId);
    expect(h.sandboxes.destroyed).toEqual(['mem-1']);
    expect(h.seen[1]).toEqual({ sandboxId: 'mem-2', created: true, lost: true, files: ['f0.txt'] });
  });

  it('a sandbox past sandboxMaxLifetimeMs is replaced', async () => {
    const h = await harness();
    const threadId = await h.run();
    await h.setRecord(threadId, { createdAt: Date.now() - LIFETIME - 1_000 });
    await h.run(threadId);
    expect(h.sandboxes.destroyed).toEqual(['mem-1']);
    expect(h.seen[1]!.lost).toBe(true);
  });

  it('a sandbox that ended on its own is replaced and its files are lost', async () => {
    const h = await harness();
    const threadId = await h.run();
    h.sandboxes.end('mem-1');
    await h.run(threadId); // this process still holds a handle on it
    expect(h.seen[1]).toEqual({ sandboxId: 'mem-2', created: true, lost: true, files: ['f0.txt'] });
    forgetSandboxHandles();
    h.sandboxes.end('mem-2');
    await h.run(threadId); // and here it does not
    expect(h.seen[2]).toEqual({ sandboxId: 'mem-3', created: true, lost: true, files: ['f0.txt'] });
  });

  it("each use pushes the sandbox's own end back, never past its lifetime", async () => {
    const h = await harness();
    const threadId = await h.run();
    expect(h.sandboxes.timeouts).toEqual([]); // it was just made with its time
    forgetSandboxHandles();
    await h.run(threadId);
    expect(h.sandboxes.timeouts).toEqual([{ sandboxId: 'mem-1', timeoutMs: IDLE + 60_000 }]);
    forgetSandboxHandles();
    await h.setRecord(threadId, { createdAt: Date.now() - LIFETIME + 5 * 60_000 });
    await h.run(threadId);
    const last = h.sandboxes.timeouts.at(-1)!.timeoutMs;
    expect(last).toBeLessThanOrEqual(5 * 60_000);
    expect(last).toBeGreaterThan(4 * 60_000);
  });

  it('deleting the thread destroys its sandbox', async () => {
    const h = await harness();
    const threadId = await h.run();
    expect((await h.runtime.deleteThread(threadId)).accepted).toBe(true);
    expect(h.sandboxes.destroyed).toEqual(['mem-1']);
    expect(await h.kv.get(sandboxKey(threadId))).toBeNull();
  });

  it('with no sandbox set up a sandbox call says so', async () => {
    const deps = { kv: new MemoryKv(), config: resolveConfig({}) } as any;
    await expect(threadSandbox({ deps, threadId: 't', agentId: null })).rejects.toThrow(
      'No sandbox: pass setupAgentCore({ tools: { sandbox } })',
    );
  });
});
