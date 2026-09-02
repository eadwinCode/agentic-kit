import { describe, expect, it } from 'bun:test';
import { simulateReadableStream, tool } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { z } from 'zod';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { bindStorage } from '../src/core/state.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { QStashQueue } from '../src/adapters/qstash.js';
import { markRequiresConfirmation } from '../src/core/engine.js';
import { resolveConfig, type AgentConfig, type RunJob } from '../src/core/types.js';
import type { RuntimeOptions } from '../src/ports/runtime.js';

/** Records which QStash API each dispatch actually used. The live server
 *  rejects Upstash-Delay on enqueue outright, so the two paths must differ. */
function fakeQStash() {
  const enqueued: Array<{ queueName: string; body: unknown }> = [];
  const published: Array<{ body: unknown; delay?: number }> = [];
  const client = {
    queue: ({ queueName }: { queueName: string }) => ({
      enqueueJSON: async (a: { url: string; body: unknown; delay?: number }) => {
        // Mirror the real API: a delayed enqueue is an error, not a no-op.
        if ((a as { delay?: number }).delay !== undefined) {
          throw new Error('Upstash-Not-Before/Upstash-Delay can not be used with enqueue');
        }
        enqueued.push({ queueName, body: a.body });
      },
    }),
    publishJSON: async (a: { url: string; body: unknown; delay?: number }) => {
      published.push({ body: a.body, delay: a.delay });
    },
  };
  return { client, enqueued, published };
}

describe('QStash dispatch (§2.8)', () => {
  const job: RunJob = { threadId: 't1', runId: 'r1', model: 'gpt-4o' };

  it('queues an immediate job, publishes a delayed one', async () => {
    const q = fakeQStash();
    const queue = new QStashQueue(q.client, { url: 'https://app.test/api/queue/agent-run' });

    await queue.enqueue(job);
    expect(q.enqueued).toEqual([{ queueName: 'agent-runs', body: job }]);
    expect(q.published).toEqual([]);

    await queue.enqueue(job, { delaySeconds: 315 });
    expect(q.enqueued).toHaveLength(1); // still just the first
    expect(q.published).toEqual([{ body: job, delay: 315 }]);
  });
});

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
  return { deps, store: bindStorage(storage, { state: {} }), runtime: await setupAgentCore(deps), storage, bus, queue, kv };
}

const parkingModel = () =>
  new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock-park',
    doStream: async () => ({
      stream: simulateReadableStream({
        chunks: [
          {
            type: 'tool-call', toolCallType: 'function', toolCallId: 'call_1',
            toolName: 'sendEmail', args: JSON.stringify({ to: 'a@b.c' }),
          },
          { type: 'finish', finishReason: 'tool-calls', usage: { promptTokens: 10, completionTokens: 5 } },
        ] as LanguageModelV1StreamPart[],
      }),
      rawCall: { rawPrompt: null, rawSettings: {} },
    }),
  });

describe('a queue that cannot schedule the HITL expiry (§2.5)', () => {
  it('still parks: the failure never reaches the run', async () => {
    const r = await makeRuntime(parkingModel());
    // Every delayed dispatch fails, the way QStash fails a delayed enqueue.
    const realEnqueue = r.queue.enqueue.bind(r.queue);
    r.deps.queue.enqueue = async (j, opts) => {
      if (opts?.delaySeconds) throw new Error('Upstash-Delay can not be used with enqueue');
      return realEnqueue(j);
    };

    const chat = r.runtime.createStreamTextAgent({
      name: 'chat',
      model: 'gpt-4o',
      tools: {
        sendEmail: markRequiresConfirmation(
          tool({ parameters: z.object({ to: z.string() }), execute: async () => ({ status: 'SENT' }) }),
        ),
      },
    });

    const ran = await chat.run({ prompt: 'hi' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    // The park stands on both homes, and nothing was marked failed.
    expect((await r.storage.threads.get(ran.threadId))!.state).toBe('WAITING_FOR_INPUT');
    expect(await r.kv.get(`agent:state:${ran.threadId}`)).toBe('WAITING_FOR_INPUT');
    expect(await r.kv.get(`agent:attempts:${ran.threadId}`)).toBeNull(); // no retry was triggered
    expect(
      r.bus.published.filter((e) => e.type === 'INPUT_REQUIRED'),
    ).toHaveLength(1);
  });
});

describe('the failure-retry dispatch (§2.8)', () => {
  it('carries the run id, so the retry is still the same run', async () => {
    const broken = new MockLanguageModelV1({
      provider: 'mock',
      modelId: 'mock-broken',
      doStream: async () => { throw new Error('provider exploded'); },
    });
    const r = await makeRuntime(broken);
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });

    const ran = await chat.run({ prompt: 'hi' });
    await chat.executeWithPolicy({ threadId: ran.threadId, runId: ran.runId, model: 'gpt-4o' });

    expect(r.queue.items).toHaveLength(2);
    expect(r.queue.items[1]).toMatchObject({ threadId: ran.threadId, runId: ran.runId });
  });
});

describe('a subagent whose model call fails (§2.7)', () => {
  it('throws instead of hanging its parent and the run lock', async () => {
    // The parent delegates once; the child's provider call blows up.
    let call = 0;
    const model = new MockLanguageModelV1({
      provider: 'mock',
      modelId: 'mock-parent-child',
      doStream: async () => {
        if (call++ === 0) {
          return {
            stream: simulateReadableStream({
              chunks: [
                {
                  type: 'tool-call', toolCallType: 'function', toolCallId: 'call_1',
                  toolName: 'spawnSubagent',
                  args: JSON.stringify({ name: 'research', instructions: 'dig' }),
                },
                { type: 'finish', finishReason: 'tool-calls', usage: { promptTokens: 10, completionTokens: 5 } },
              ] as LanguageModelV1StreamPart[],
            }),
            rawCall: { rawPrompt: null, rawSettings: {} },
          };
        }
        throw new Error('subagent provider exploded');
      },
    });

    const r = await makeRuntime(model);
    const chat = r.runtime.createStreamTextAgent({
      name: 'chat', model: 'gpt-4o', subagents: true,
    });

    const ran = await chat.run({ prompt: 'hi' });
    await chat.executeWithPolicy({ threadId: ran.threadId, runId: ran.runId, model: 'gpt-4o' });

    // It came back at all, and let go of the lock.
    expect(await r.kv.get(`agent:lock:${ran.threadId}`)).toBeNull();
    // The child was recorded as failed rather than silently completing.
    expect(r.bus.published.some((e) => e.type === 'SUBAGENT_FAILED')).toBe(true);
  }, 10_000);
});
