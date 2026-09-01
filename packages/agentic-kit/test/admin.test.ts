import { describe, expect, it } from 'bun:test';
import { simulateReadableStream } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { AdminUnavailableError } from '../src/core/admin.js';
import { resolveConfig } from '../src/core/types.js';
import type { RuntimePorts } from '../src/ports/runtime.js';

const model = (text = 'ok') =>
  new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock',
    doStream: async () => ({
      stream: simulateReadableStream({
        chunks: [
          { type: 'text-delta', textDelta: text },
          { type: 'finish', finishReason: 'stop', usage: { promptTokens: 10, completionTokens: 5 } },
        ] as LanguageModelV1StreamPart[],
      }),
      rawCall: { rawPrompt: null, rawSettings: {} },
    }),
  });

function makeRuntime(m: any = model()) {
  const storage = new MemoryStorage();
  const deps: RuntimePorts = {
    storage, bus: new MemoryBus(), queue: new MemoryQueue(), kv: new MemoryKv(),
    resolveModel: () => ({ instance: () => m, contextWindow: 128_000 }),
    config: resolveConfig(),
  };
  return { deps, runtime: setupAgentCore(deps), storage, queue: deps.queue as MemoryQueue };
}

describe('run records (§2.9)', () => {
  it('opens on run() and closes with timing, tokens and steps', async () => {
    const r = makeRuntime();
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });

    const ran = await chat.run({ prompt: 'hi' });
    const open = (await r.runtime.admin.getRun(ran.runId!))!;
    expect(open.run).toMatchObject({
      id: ran.runId, threadId: ran.threadId, agent: 'chat', model: 'gpt-4o', state: 'RUNNING',
    });
    expect(open.run.endedAt ?? null).toBeNull();

    await r.runtime.worker.handleJob(r.queue.items[0]!);

    const done = (await r.runtime.admin.getRun(ran.runId!))!;
    expect(done.run.state).toBe('COMPLETED');
    expect(done.run.stopReason).toBe('completed');
    expect(done.run.totalTokens).toBe(15);
    expect(done.run.steps).toBe(1);
    expect(typeof done.run.durationMs).toBe('number');
    // The dispatch carried its enqueue time, so the wait is measured.
    expect(typeof done.run.queuedMs).toBe('number');
  });

  it('keeps the reason a run failed, which used to be dropped', async () => {
    const broken = new MockLanguageModelV1({
      provider: 'mock', modelId: 'broken',
      doStream: async () => { throw new Error('provider is down'); },
    });
    const r = makeRuntime(broken);
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const ran = await chat.run({ prompt: 'hi' });

    // Exhaust the retry budget so the run finalises FAILED.
    for (let i = 0; i < 4; i++) {
      await chat.executeWithPolicy({ threadId: ran.threadId, runId: ran.runId, model: 'gpt-4o' });
    }

    const detail = (await r.runtime.admin.getRun(ran.runId!))!;
    expect(detail.run.state).toBe('FAILED');
    expect(detail.run.error).toBe('provider is down');
  });

  it('records a step marker per loop iteration', async () => {
    const r = makeRuntime();
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const ran = await chat.run({ prompt: 'hi' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    const steps = await r.runtime.admin.listSteps(ran.threadId, ran.runId);
    expect(steps).toHaveLength(1);
    expect(steps[0]).toMatchObject({
      runId: ran.runId, agentId: null, index: 1, finishReason: 'stop', tools: [],
    });
    expect(steps[0]!.tokens.totalTokens).toBe(15);
    expect(typeof steps[0]!.durationMs).toBe('number');
  });
});

describe('cross-thread views (§2.9)', () => {
  it('summarises runs with percentiles and state counts', async () => {
    const r = makeRuntime();
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    for (let i = 0; i < 3; i++) {
      const ran = await chat.run({ prompt: `hi ${i}` });
      await r.runtime.worker.handleJob(r.queue.items.at(-1)!);
      void ran;
    }

    const stats = await r.runtime.admin.stats();
    expect(stats.total).toBe(3);
    expect(stats.byState.COMPLETED).toBe(3);
    expect(stats.byStopReason.completed).toBe(3);
    expect(stats.tokens.totalTokens).toBe(45);
    expect(stats.duration).not.toBeNull();
    expect(stats.failed).toBe(0);

    const view = await r.runtime.admin.overview();
    expect(view.threads.COMPLETED).toBe(3);
    expect(view.active).toEqual([]);
  });

  it('says plainly when the adapter has no admin queries', async () => {
    const r = makeRuntime();
    // An adapter that implements only what running agents needs (§3.2).
    delete (r.deps.storage as any).admin;
    expect(r.runtime.admin.available).toBe(false);
    await expect(r.runtime.admin.stats()).rejects.toBeInstanceOf(AdminUnavailableError);

    // Per-thread reads keep working without it.
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const ran = await chat.run({ prompt: 'hi' });
    expect(await r.runtime.admin.listRunsByThread(ran.threadId)).toHaveLength(1);
  });
});
