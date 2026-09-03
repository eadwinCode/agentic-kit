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

/** A model that reports an OpenAI-style cache hit. Note promptTokens INCLUDES
 *  the cached ones — that is what makes double-counting easy. */
function cachingModel() {
  return new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock-cache',
    doStream: async () => {
      const chunks: LanguageModelV1StreamPart[] = [
        { type: 'text-delta', textDelta: 'cached hello' },
        {
          type: 'finish',
          finishReason: 'stop',
          usage: { promptTokens: 1200, completionTokens: 40 },
          providerMetadata: { openai: { cachedPromptTokens: 1024 } },
        },
      ];
      return {
        stream: simulateReadableStream({ chunks }),
        rawCall: { rawPrompt: null, rawSettings: {} },
      };
    },
  });
}

/** Captures the prompt as the PROVIDER receives it — after the SDK has
 *  converted messages and the `system:` parameter. Asserting on what we passed
 *  in proves nothing; only this shows whether a breakpoint survived. */
function capturingModel(seen: { prompt?: any }) {
  return new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock-capture',
    doStream: async ({ prompt }: any) => {
      seen.prompt = prompt;
      return {
        stream: simulateReadableStream({
          chunks: [
            { type: 'text-delta', textDelta: 'ok' },
            { type: 'finish', finishReason: 'stop', usage: { promptTokens: 5, completionTokens: 1 } },
          ] as LanguageModelV1StreamPart[],
        }),
        rawCall: { rawPrompt: null, rawSettings: {} },
      };
    },
  });
}

const cacheControlOf = (entry: any) =>
  entry?.providerMetadata?.anthropic?.cacheControl ??
  (Array.isArray(entry?.content)
    ? entry.content.at(-1)?.providerMetadata?.anthropic?.cacheControl
    : undefined) ??
  null;

describe('prompt caching reaches the provider (§2.6)', () => {
  // The SDK's `system:` string parameter is converted to a bare
  // { role, content } with no metadata channel, so a system prompt passed that
  // way CANNOT hold a breakpoint — and it is the largest, most stable part of
  // the prompt, the thing caching exists for.
  it('carries the agent system prompt as a stamped message', async () => {
    const seen: { prompt?: any } = {};
    const { runtime, store } = await makeRuntime(capturingModel(seen));
    const agent = runtime.createStreamTextAgent({
      name: 'sys',
      model: 'mock',
      system: 'You are a big expensive persona worth caching.',
    } as any);

    const threadId = (await store.threads.create({ model: 'mock' })).id;
    const started = await agent.run({ threadId, prompt: 'hi' });
    await runtime.worker.handleJob({ threadId, runId: started.runId, model: 'mock', agent: 'sys' });

    const system = seen.prompt.find((m: any) => m.role === 'system');
    expect(system).toBeDefined();
    expect(system.content).toContain('big expensive persona');
    expect(cacheControlOf(system)).toEqual({ type: 'ephemeral' });
    // ... and it is not ALSO sent as the plain parameter, which would repeat it
    expect(seen.prompt.filter((m: any) => m.role === 'system')).toHaveLength(1);
  });

  it('leaves the system prompt as a plain parameter when caching is off', async () => {
    const seen: { prompt?: any } = {};
    const { runtime, store } = await makeRuntime(capturingModel(seen), { promptCaching: false });
    const agent = runtime.createStreamTextAgent({
      name: 'sys2',
      model: 'mock',
      system: 'plain persona',
    } as any);

    const threadId = (await store.threads.create({ model: 'mock' })).id;
    const started = await agent.run({ threadId, prompt: 'hi' });
    await runtime.worker.handleJob({ threadId, runId: started.runId, model: 'mock', agent: 'sys2' });

    const system = seen.prompt.find((m: any) => m.role === 'system');
    expect(system.content).toBe('plain persona');
    expect(cacheControlOf(system)).toBeNull();
  });
});

describe('cached prompt tokens (§4)', () => {
  // The cache hit exists only in provider metadata. If a step drops it on the
  // way out of the model, every cached prompt is billed at full input price and
  // no counter anywhere in the system can ever be non-zero.
  it('carries a cache hit from the model into the step record and thread total', async () => {
    const { deps, runtime, storage, store } = await makeRuntime(cachingModel());
    const agent = runtime.createStreamTextAgent({ name: 'cacher', model: 'mock' });
    const threadId = (await store.threads.create({ model: 'mock' })).id;

    const started = await agent.run({ threadId, prompt: 'hi' });
    expect(started.accepted).toBe(true);
    await runtime.worker.handleJob({
      threadId,
      runId: started.runId,
      model: 'mock',
      agent: 'cacher',
    });

    const total = await storage.usage.total(threadId);
    expect(total.cachedInputTokens).toBe(1024);
    // 1200 reported minus the 1024 already cached — NOT 1200 + 1024.
    expect(total.inputTokens).toBe(176);
    expect(total.outputTokens).toBe(40);

    const steps = await deps.admin!.steps.listByThread(threadId);
    expect(steps.length).toBeGreaterThan(0);
    expect(steps[0].cachedInputTokens).toBe(1024);
    expect(steps[0].inputTokens).toBe(176);
  });
});

describe('thread usage (§4)', () => {
  it('sums every model call, per thread', async () => {
    const storage = new MemoryStorage();
    const row = {
      runId: 'r1', agentId: null, agentName: 'chat', model: 'gpt-4o', modelId: 'gpt-4o',
      kind: 'step' as const, step: 1, outcome: 'finished' as const,
      inputTokens: 10, cacheReadInputTokens: 4, cacheWriteInputTokens: 0,
      outputTokens: 6, reasoningTokens: 0, totalTokens: 20,
      cost: { micros: 25, currency: 'USD', source: 'table' as const },
    };
    await storage.usage.record('t1', row);
    await storage.usage.record('t1', { ...row, step: 2 });
    await storage.usage.record('t2', row);

    const t1 = await storage.usage.total('t1', {});
    expect(t1.inputTokens).toBe(20);
    expect(t1.cachedInputTokens).toBe(8);
    expect(t1.totalTokens).toBe(40);
    // Money is summed alongside the tokens (§4).
    expect(t1.costMicros).toBe(50);
    expect(t1.currency).toBe('USD');
    expect(t1.unpriced).toBe(0);
    // And grouped into the lines a bill is made of: one agent, one model.
    expect(t1.lines).toEqual([
      {
        agentId: null, agentName: 'chat', model: 'gpt-4o', modelId: 'gpt-4o',
        inputTokens: 20, cacheReadInputTokens: 8, cacheWriteInputTokens: 0,
        outputTokens: 12, reasoningTokens: 0, calls: 2, estimated: 0, costMicros: 50,
      },
    ]);
    // The run filter narrows to one dispatched run.
    expect((await storage.usage.total('t1', { runId: 'r1' })).totalTokens).toBe(40);
    expect((await storage.usage.total('t1', { runId: 'other' })).totalTokens).toBe(0);
    // A thread with nothing recorded reads as zeroes, never null.
    const none = await storage.usage.total('unknown', {});
    expect(none.totalTokens).toBe(0);
    expect(none.costMicros).toBe(0);
    expect(none.lines).toEqual([]);
  });

  it('getThreadUsage reports what a finished run actually spent', async () => {
    const { runtime, queue } = await makeRuntime(replyModel('hello'));
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });

    const ran = await chat.run({ prompt: 'hi' });
    await runtime.worker.handleJob(queue.items[0]!);

    const usage = (await runtime.getThreadUsage(ran.threadId))!;
    expect(usage.tokens.inputTokens).toBe(120);
    expect(usage.tokens.cachedInputTokens).toBe(0);
    expect(usage.tokens.outputTokens).toBe(30);
    expect(usage.tokens.totalTokens).toBe(150);
    // Nothing priced it, so the money is zero AND the call is counted as
    // unpriced — the two are not the same thing (§4).
    expect(usage.tokens.costMicros).toBe(0);
    expect(usage.tokens.unpriced).toBe(1);
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
