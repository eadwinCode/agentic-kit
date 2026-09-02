import { describe, expect, it } from 'bun:test';
import { simulateReadableStream, tool } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { z } from 'zod';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { bindStorage } from '../src/core/state.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { resolveConfig, type AgentConfig } from '../src/core/types.js';
import type { RuntimeOptions } from '../src/ports/runtime.js';

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

async function makeRuntime(m: any = model(), config: Partial<AgentConfig> = {}) {
  const storage = new MemoryStorage();
  const deps: RuntimeOptions = {
    storage, bus: new MemoryBus(), queue: new MemoryQueue(), kv: new MemoryKv(),
    // Isolated per test: the default store is a file on disk.
    admin: new MemoryAdminStore(),
    resolveModel: () => ({ instance: () => m, contextWindow: 128_000 }),
    config: resolveConfig(config),
  };
  return { deps, store: bindStorage(storage, { state: {} }), runtime: await setupAgentCore(deps), storage, queue: deps.queue as MemoryQueue };
}

describe('run records (§2.9)', () => {
  it('opens on run() and closes with timing, tokens and steps', async () => {
    const r = await makeRuntime();
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
    const r = await makeRuntime(broken);
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
    const r = await makeRuntime();
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const ran = await chat.run({ prompt: 'hi' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    const steps = await r.runtime.admin.listSteps(ran.runId!);
    expect(steps).toHaveLength(1);
    expect(steps[0]).toMatchObject({
      runId: ran.runId, agentId: null, index: 1, finishReason: 'stop', tools: [],
      totalTokens: 15,
    });
    expect(typeof steps[0]!.durationMs).toBe('number');
  });
});

describe('cross-thread views (§2.9)', () => {
  it('summarises runs with percentiles and state counts', async () => {
    const r = await makeRuntime();
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
    // Both come from the platform's own store — a dashboard never reads the
    // caller's database (§2.9).
    expect(view.threads.COMPLETED).toBe(3);
    expect(view.runsByState.COMPLETED).toBe(3);
    expect(view.active).toEqual([]);
  });

  it("reads a thread's runs without touching the caller's storage", async () => {
    const r = await makeRuntime();
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const ran = await chat.run({ prompt: 'hi' });

    expect(await r.runtime.admin.listRunsByThread(ran.threadId)).toHaveLength(1);
    // Nothing about runs is in the caller's Storage any more (§2.9).
    expect('runs' in (r.storage as object)).toBe(false);
  });
});

describe('what a step produced (§2.9)', () => {
  const toolingModel = () => {
    let call = 0;
    return new MockLanguageModelV1({
      provider: 'mock', modelId: 'mock-tooling',
      doStream: async () => {
        const first = call++ === 0;
        const chunks: LanguageModelV1StreamPart[] = first
          ? [{
              type: 'tool-call', toolCallType: 'function', toolCallId: 'c1',
              toolName: 'lookup', args: JSON.stringify({ q: 'tin' }),
            }]
          : [{ type: 'text-delta', textDelta: 'tin is a metal' }];
        chunks.push({
          type: 'finish',
          finishReason: first ? 'tool-calls' : 'stop',
          usage: { promptTokens: 100, completionTokens: 20 },
        });
        return {
          stream: simulateReadableStream({ chunks }),
          rawCall: { rawPrompt: null, rawSettings: {} },
        };
      },
    });
  };

  const withLookup = (r: any) =>
    r.runtime.createStreamTextAgent({
      name: 'chat', model: 'gpt-4o',
      tools: {
        lookup: tool({
          parameters: z.object({ q: z.string() }),
          execute: async ({ q }) => ({ found: `${q}: Sn, atomic number 50` }),
        }),
      },
    });

  it('records the text, the tool payloads, and the token split per step', async () => {
    const r = await makeRuntime(toolingModel());
    const chat = withLookup(r);
    const ran = await chat.run({ prompt: 'what is tin' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    const steps = await r.runtime.admin.listSteps(ran.runId!);
    expect(steps).toHaveLength(2);

    // Step 1 ran a tool: its arguments AND its result are both kept, which is
    // the difference between "a tool ran" and "here is what it did".
    expect(steps[0]).toMatchObject({
      finishReason: 'tool-calls', tools: ['lookup'],
      inputTokens: 100, outputTokens: 20, totalTokens: 120,
    });
    expect(steps[0]!.toolCalls).toEqual([
      { toolName: 'lookup', args: { q: 'tin' }, result: { found: 'tin: Sn, atomic number 50' } },
    ]);

    // Step 2 produced text.
    expect(steps[1]!.text).toBe('tin is a metal');
    expect(steps[1]!.tools).toEqual([]);
  });

  it('keeps only timings when recordPayloads is off', async () => {
    const r = await makeRuntime(toolingModel(), { recordPayloads: false });
    const chat = withLookup(r);
    const ran = await chat.run({ prompt: 'what is tin' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    const steps = await r.runtime.admin.listSteps(ran.runId!);
    // Tool NAMES stay — they are not payloads — but nothing a tool carried does.
    expect(steps[0]!.tools).toEqual(['lookup']);
    expect(steps[0]!.toolCalls ?? []).toEqual([]);
    expect(steps[1]!.text ?? null).toBeNull();
    expect(steps[0]!.totalTokens).toBe(120);
  });

  it('caps a large value rather than storing all of it', async () => {
    const huge = 'x'.repeat(10_000);
    const model = new MockLanguageModelV1({
      provider: 'mock', modelId: 'mock-huge',
      doStream: async () => ({
        stream: simulateReadableStream({
          chunks: [
            { type: 'text-delta', textDelta: huge },
            { type: 'finish', finishReason: 'stop', usage: { promptTokens: 10, completionTokens: 5 } },
          ] as LanguageModelV1StreamPart[],
        }),
        rawCall: { rawPrompt: null, rawSettings: {} },
      }),
    });
    const r = await makeRuntime(model, { payloadCapChars: 100 });
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const ran = await chat.run({ prompt: 'go' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    const [step] = await r.runtime.admin.listSteps(ran.runId!);
    expect(step!.text!.length).toBe(101); // 100 plus the ellipsis
    expect(step!.text!.endsWith('…')).toBe(true);
  });
});

describe('what a run was dispatched with (§2.9)', () => {
  it('records the prompt, budget and state that started it', async () => {
    const r = await makeRuntime();
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const ran = await chat.run({
      prompt: 'summarise the quarter', tokenBudget: 5_000, state: { orgId: 'acme' },
    });

    const { run } = (await r.runtime.admin.getRun(ran.runId!))!;
    // A dashboard can show a run was slow; this is what it was slow AT.
    expect(run).toMatchObject({
      prompt: 'summarise the quarter', tokenBudget: 5_000, runState: { orgId: 'acme' },
    });
  });

  it('holds none of it when recordPayloads is off', async () => {
    const r = await makeRuntime(model(), { recordPayloads: false });
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const ran = await chat.run({ prompt: 'sensitive', state: { orgId: 'acme' } });

    const { run } = (await r.runtime.admin.getRun(ran.runId!))!;
    expect(run.prompt ?? null).toBeNull();
    expect(run.runState ?? null).toBeNull();
    // Timings and identity are not payloads and stay either way.
    expect(run).toMatchObject({ agent: 'chat', model: 'gpt-4o', state: 'RUNNING' });
  });
});

describe('threads rolled up (§2.9)', () => {
  it('sums a thread\'s runs, steps and tokens across every run on it', async () => {
    const r = await makeRuntime();
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });

    const first = await chat.run({ prompt: 'one' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);
    // A second turn on the SAME thread: a thread has many runs, and the
    // rollup is what makes that legible.
    await chat.run({ threadId: first.threadId, prompt: 'two' });
    await r.runtime.worker.handleJob(r.queue.items.at(-1)!);

    const [summary] = await r.runtime.admin.listThreads();
    expect(summary).toMatchObject({
      id: first.threadId, state: 'COMPLETED', runs: 2, steps: 2,
      prompt: 'one',   // what STARTED it, not the latest turn
    });
    expect(summary!.tokens.totalTokens).toBe(30);
    expect(summary!.durationMs).toBeGreaterThan(0);

    const detail = (await r.runtime.admin.getThread(first.threadId))!;
    expect(detail.runs).toHaveLength(2);
    // Every step on the thread, across both runs, in one list.
    expect(detail.steps).toHaveLength(2);
    expect(detail.steps.every((s) => s.threadId === first.threadId)).toBe(true);
  });

  it('gathers a nested run\'s steps into the same thread timeline', async () => {
    const r = await makeRuntime();
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const ran = await chat.run({ prompt: 'hi' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    const detail = (await r.runtime.admin.getThread(ran.threadId))!;
    // Steps carry their own run id, so a timeline can still separate them.
    expect(new Set(detail.steps.map((s) => s.runId)).size).toBe(detail.runs.length);
  });

  it('is null for a thread nothing ever ran on', async () => {
    const r = await makeRuntime();
    expect(await r.runtime.admin.getThread('never-existed')).toBeNull();
  });
});
