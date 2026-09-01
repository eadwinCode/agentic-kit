import { describe, expect, it } from 'bun:test';
import { simulateReadableStream } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { bindStorage } from '../src/core/state.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { contextUsage } from '../src/core/context.js';
import { resolveConfig, type AgentConfig } from '../src/core/types.js';
import type { RuntimeOptions } from '../src/ports/runtime.js';

function replyModel(text: string) {
  return new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock-reply',
    doStream: async () => {
      const chunks: LanguageModelV1StreamPart[] = [
        { type: 'text-delta', textDelta: text },
        { type: 'finish', finishReason: 'stop', usage: { promptTokens: 120, completionTokens: 30 } },
      ];
      return {
        stream: simulateReadableStream({ chunks }),
        rawCall: { rawPrompt: null, rawSettings: {} },
      };
    },
  });
}

async function makeRuntime(model: any, config: Partial<AgentConfig> = {}) {
  const storage = new MemoryStorage();
  const bus = new MemoryBus();
  const queue = new MemoryQueue();
  const kv = new MemoryKv();
  const deps: RuntimeOptions = {
    storage, bus, queue, kv,
    // Isolated per test: the default store is a file on disk.
    admin: new MemoryAdminStore(),
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
    config: resolveConfig(config),
  };
  return { deps, store: bindStorage(storage, { state: {} }), runtime: await setupAgentCore(deps), storage, queue };
}

describe('thread usage (§4)', () => {
  it('sums every run segment, per thread', async () => {
    const storage = new MemoryStorage();
    const row = { agentId: null, inputTokens: 10, cachedInputTokens: 4, outputTokens: 6, totalTokens: 20 };
    await storage.usage.record('t1', row);
    await storage.usage.record('t1', row);
    await storage.usage.record('t2', row);

    expect(await storage.usage.total('t1')).toEqual({
      inputTokens: 20, cachedInputTokens: 8, outputTokens: 12, totalTokens: 40,
    });
    // A thread with nothing recorded reads as zeroes, never null.
    expect(await storage.usage.total('unknown')).toEqual({
      inputTokens: 0, cachedInputTokens: 0, outputTokens: 0, totalTokens: 0,
    });
  });

  it('getThreadUsage reports what a finished run actually spent', async () => {
    const { runtime, queue } = await makeRuntime(replyModel('hello'));
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });

    const ran = await chat.run({ prompt: 'hi' });
    await runtime.worker.handleJob(queue.items[0]!);

    const usage = (await runtime.getThreadUsage(ran.threadId))!;
    expect(usage.tokens).toEqual({
      inputTokens: 120, cachedInputTokens: 0, outputTokens: 30, totalTokens: 150,
    });
    expect(usage.model).toBe('gpt-4o');
    expect(usage.context.messages).toBe(2); // the prompt and the reply
  });

  it('getThreadUsage is null for a thread that does not exist', async () => {
    const { runtime } = await makeRuntime(replyModel('hello'));
    expect(await runtime.getThreadUsage('nope')).toBeNull();
  });
});

describe('context usage (§2.6)', () => {
  it('measures the same budget compaction acts on, and grows with history', async () => {
    const { deps, store, runtime, queue } = await makeRuntime(replyModel('hello'));
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const ran = await chat.run({ prompt: 'hi' });

    const cfg = deps.config!;
    const budget = 128_000 - cfg.contextOutputReserveTokens!;
    const empty = await contextUsage({ ...deps, storage: store } as any, ran.threadId, 'gpt-4o');
    expect(empty.budgetTokens).toBe(budget);
    expect(empty.compactAtTokens).toBe(Math.floor(budget * cfg.compactionTrigger!));
    expect(empty.messages).toBe(1);
    expect(empty.usedTokens).toBeGreaterThan(0);

    await runtime.worker.handleJob(queue.items[0]!);

    const after = await contextUsage({ ...deps, storage: store } as any, ran.threadId, 'gpt-4o');
    expect(after.messages).toBe(2);
    expect(after.usedTokens).toBeGreaterThan(empty.usedTokens);
  });

  it('honours a model window smaller than the ceiling', async () => {
    const storage = new MemoryStorage();
    const deps: RuntimeOptions = {
      storage, bus: new MemoryBus(), queue: new MemoryQueue(), kv: new MemoryKv(),
      // Isolated per test: the default store is a file on disk.
      admin: new MemoryAdminStore(),
      resolveModel: () => ({ instance: () => null as any, contextWindow: 32_000 }),
      config: resolveConfig({ contextOutputReserveTokens: 2_000 }),
    };
    const scoped = bindStorage(storage, { state: {} });
    const thread = await scoped.threads.create({ model: 'small' });
    expect(
      (await contextUsage({ ...deps, storage: scoped } as any, thread.id, 'small')).budgetTokens,
    ).toBe(30_000);
  });
});
