import { describe, expect, it } from 'bun:test';
import { simulateReadableStream } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { SqliteAdminStore } from '../src/admin/sqlite.js';
import { openSqlite } from '../src/adapters/sqlite.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { resolveConfig, type AgentConfig } from '../src/core/types.js';
import type { AdminStore } from '../src/ports/admin.js';
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

async function makeRuntime(config: Partial<AgentConfig> = {}, admin: AdminStore = new MemoryAdminStore()) {
  const queue = new MemoryQueue();
  const deps: RuntimeOptions = {
    storage: new MemoryStorage(), admin, bus: new MemoryBus(), queue, kv: new MemoryKv(),
    resolveModel: () => ({ instance: () => model(), contextWindow: 128_000 }),
    config: resolveConfig({ providerOptions: { openai: { tier: 'flex' } }, ...config }),
  };
  return { runtime: await setupAgentCore(deps), queue, admin };
}

describe('what started a thread (§2.9)', () => {
  it('is recorded on the thread on first sight, with every dispatch parameter', async () => {
    const { runtime, queue } = await makeRuntime();
    const chat = runtime.createStreamTextAgent({
      name: 'chat', model: 'gpt-4o', providerOptions: { anthropic: { effort: 'high' } },
    });
    const ran = await chat.run({
      prompt: 'send the quarterly report', tokenBudget: 5_000,
      state: { orgId: 'acme' }, providerOptions: { openai: { tier: 'priority' } },
    });
    const [thread] = await runtime.admin.listThreads();
    expect(thread!.startedWith).toMatchObject({
      runId: ran.runId, agent: 'chat', model: 'gpt-4o',
      prompt: 'send the quarterly report', tokenBudget: 5_000, state: { orgId: 'acme' },
      // The three levels merged, the input winning per namespace (§3.1)
      providerOptions: { openai: { tier: 'priority' }, anthropic: { effort: 'high' } },
    });
    expect(thread!.startedWith!.at).toBeInstanceOf(Date);
    expect(thread!.prompt).toBe('send the quarterly report');
    // The run record carries the same provider options next to its prompt
    const run = (await runtime.admin.getRun(ran.runId!))!.run;
    expect(run.providerOptions).toEqual({ openai: { tier: 'priority' }, anthropic: { effort: 'high' } });

    // A second run on the thread never overwrites what started it
    await runtime.worker.handleJob(queue.items[0]!);
    const again = await chat.run({ threadId: ran.threadId, prompt: 'and again' });
    const [after] = await runtime.admin.listThreads();
    expect(after!.startedWith!.runId).toBe(ran.runId!);
    expect(after!.startedWith!.prompt).toBe('send the quarterly report');
    expect(again.runId).not.toBe(ran.runId);
  });

  it('keeps only the identity when recordPayloads is off', async () => {
    const { runtime } = await makeRuntime({ recordPayloads: false });
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const ran = await chat.run({ prompt: 'private', state: { orgId: 'acme' }, providerOptions: { openai: { x: 1 } } });
    const [thread] = await runtime.admin.listThreads();
    expect(thread!.startedWith).toMatchObject({ runId: ran.runId, agent: 'chat', model: 'gpt-4o' });
    expect(thread!.startedWith!.prompt).toBeUndefined();
    expect(thread!.startedWith!.state).toBeUndefined();
    expect(thread!.startedWith!.providerOptions).toBeUndefined();
    const run = (await runtime.admin.getRun(ran.runId!))!.run;
    expect(run.providerOptions ?? null).toBeNull();
  });

  it('records nothing when no level sets provider options', async () => {
    const { runtime } = await makeRuntime({ providerOptions: undefined });
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const ran = await chat.run({ prompt: 'plain' });
    const [thread] = await runtime.admin.listThreads();
    expect(thread!.startedWith!.providerOptions).toBeNull();
    expect((await runtime.admin.getRun(ran.runId!))!.run.providerOptions).toBeNull();
  });

  it('survives the SQLite store, including its date', async () => {
    const store = new SqliteAdminStore(await openSqlite(':memory:'));
    const { runtime } = await makeRuntime({}, store);
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const ran = await chat.run({ prompt: 'durable', state: { orgId: 'acme' } });
    // The transition upserts that follow must not clear it
    await store.threads.upsert({ id: ran.threadId, state: 'COMPLETED', model: 'gpt-4o' });
    const [thread] = await store.threads.list({});
    expect(thread!.state).toBe('COMPLETED');
    expect(thread!.startedWith).toMatchObject({ runId: ran.runId, prompt: 'durable', state: { orgId: 'acme' } });
    expect(thread!.startedWith!.at).toBeInstanceOf(Date);
    expect((await store.runs.get(ran.runId!))!.providerOptions).toEqual({ openai: { tier: 'flex' } });
  });
});
