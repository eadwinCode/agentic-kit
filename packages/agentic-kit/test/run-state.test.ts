import { describe, expect, it } from 'bun:test';
import { simulateReadableStream, tool } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { z } from 'zod';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { resolveConfig } from '../src/core/types.js';
import type { StorageContext } from '../src/core/state.js';
import type { RuntimeOptions } from '../src/ports/runtime.js';

const model = () =>
  new MockLanguageModelV1({
    provider: 'mock', modelId: 'mock',
    doStream: async () => ({
      stream: simulateReadableStream({
        chunks: [
          { type: 'text-delta', textDelta: 'ok' },
          { type: 'finish', finishReason: 'stop', usage: { promptTokens: 10, completionTokens: 5 } },
        ] as LanguageModelV1StreamPart[],
      }),
      rawCall: { rawPrompt: null, rawSettings: {} },
    }),
  });

/** Records the context every storage call received, which is the only way to
 *  tell "the port accepts a state" from "the state actually arrives". */
function spyingStorage() {
  const seen: Array<{ method: string; ctx: StorageContext }> = [];
  const inner = new MemoryStorage();
  const wrap = (group: string, obj: any) =>
    Object.fromEntries(
      Object.entries(obj).map(([name, fn]) => [
        name,
        typeof fn === 'function'
          ? (...args: any[]) => {
              const ctx = args[args.length - 1];
              if (ctx && typeof ctx === 'object' && 'state' in ctx) {
                seen.push({ method: `${group}.${name}`, ctx });
              }
              return (fn as Function).apply(obj, args);
            }
          : fn,
      ]),
    );
  const storage = {
    threads: wrap('threads', inner.threads),
    messages: wrap('messages', inner.messages),
    events: wrap('events', inner.events),
    usage: wrap('usage', inner.usage),
  } as any;
  return { storage, seen, inner };
}

describe('run state (§2.10)', () => {
  const make = async () => {
    const spy = spyingStorage();
    const queue = new MemoryQueue();
    const deps: RuntimeOptions = {
      storage: spy.storage, bus: new MemoryBus(), queue, kv: new MemoryKv(),
      // Isolated per test: the default store is a file on disk.
      admin: new MemoryAdminStore(),
      resolveModel: () => ({ instance: () => model(), contextWindow: 128_000 }),
      config: resolveConfig(),
    };
    return { ...spy, queue, runtime: await setupAgentCore(deps) };
  };

  it('reaches every storage call a run makes', async () => {
    const r = await make();
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });

    await chat.run({ prompt: 'hi', state: { orgId: 'acme', userId: 'u1' } });

    expect(r.seen.length).toBeGreaterThan(0);
    // Not one call may be missing it, or a tenant-scoped implementation would
    // silently read or write across tenants.
    for (const call of r.seen) {
      expect(call.ctx.state).toEqual({ orgId: 'acme', userId: 'u1' });
    }
    // Including the ones that are not obviously "per run".
    expect(r.seen.map((c) => c.method)).toContain('threads.create');
    expect(r.seen.map((c) => c.method)).toContain('messages.append');
  });

  it('rides the dispatch, so a worker rehydrates it', async () => {
    const r = await make();
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    await chat.run({ prompt: 'hi', state: { orgId: 'acme' } });

    expect(r.queue.items[0]!.state).toEqual({ orgId: 'acme' });

    // A worker that never saw the caller picks the job up cold.
    r.seen.length = 0;
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    expect(r.seen.length).toBeGreaterThan(0);
    for (const call of r.seen) expect(call.ctx.state).toEqual({ orgId: 'acme' });
  });

  it('is an empty object when a caller supplies none', async () => {
    const r = await make();
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    await chat.run({ prompt: 'hi' });
    for (const call of r.seen) expect(call.ctx.state).toEqual({});
  });

  it('does not leak between runs on the same runtime', async () => {
    const r = await make();
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    await chat.run({ prompt: 'a', state: { orgId: 'one' } });
    r.seen.length = 0;
    await chat.run({ prompt: 'b', state: { orgId: 'two' } });
    for (const call of r.seen) expect(call.ctx.state).toEqual({ orgId: 'two' });
  });
});

describe('run state reaches tools (§2.10)', () => {
  it('arrives beside the tool call id, for every tool', async () => {
    const seen: unknown[] = [];
    const calling = new MockLanguageModelV1({
      provider: 'mock', modelId: 'calling',
      doStream: async ({ prompt }: any) => {
        const answered = (prompt ?? []).some((m: any) => m.role === 'tool');
        const chunks: LanguageModelV1StreamPart[] = answered
          ? [{ type: 'text-delta', textDelta: 'done' }]
          : [{
              type: 'tool-call', toolCallType: 'function', toolCallId: 'c1',
              toolName: 'whoami', args: '{}',
            }];
        chunks.push({
          type: 'finish',
          finishReason: answered ? 'stop' : 'tool-calls',
          usage: { promptTokens: 10, completionTokens: 5 },
        });
        return {
          stream: simulateReadableStream({ chunks }),
          rawCall: { rawPrompt: null, rawSettings: {} },
        };
      },
    });
    const queue = new MemoryQueue();
    const deps: RuntimeOptions = {
      storage: new MemoryStorage(), bus: new MemoryBus(), queue, kv: new MemoryKv(),
      // Isolated per test: the default store is a file on disk.
      admin: new MemoryAdminStore(),
      resolveModel: () => ({ instance: () => calling, contextWindow: 128_000 }),
      config: resolveConfig(),
    };
    const runtime = await setupAgentCore(deps);
    const chat = runtime.createStreamTextAgent({
      name: 'chat', model: 'gpt-4o',
      tools: {
        // A plain tool: not marked for approval, still needs to know which
        // tenant it is acting for.
        whoami: tool({
          parameters: z.object({}),
          execute: async (_args: unknown, opts: any) => {
            seen.push(opts?.state);
            return { ok: true };
          },
        }),
      },
    });

    await chat.run({ prompt: 'hi', state: { orgId: 'acme' } });
    await runtime.worker.handleJob(queue.items[0]!);

    expect(seen).toEqual([{ orgId: 'acme' }]);
  });
});
