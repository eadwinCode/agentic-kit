import { describe, expect, it } from 'bun:test';
import { simulateReadableStream } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { runIdKey } from '../src/core/keys.js';
import { resolveConfig, type AgentConfig } from '../src/core/types.js';
import type { RuntimePorts } from '../src/ports/runtime.js';

/** Pull the newest user message out of the SDK-level prompt so the mock can
 *  answer the message it was actually given — that is how these tests tell
 *  "replied to A" apart from "replied to B". */
function lastUserText(prompt: any): string {
  const users = (prompt ?? []).filter((m: any) => m.role === 'user');
  const content = users.at(-1)?.content;
  if (typeof content === 'string') return content;
  return (content ?? []).map((p: any) => p?.text ?? '').join('');
}

/** A model whose call takes `latencyMs` and, like a real provider, fails the
 *  moment the run's abort signal fires. `aborted` records that it did. */
function slowModel(state: { aborted: boolean }, latencyMs = 300) {
  return new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock-slow',
    doStream: async ({ prompt, abortSignal }: any) => {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(resolve, latencyMs);
        abortSignal?.addEventListener('abort', () => {
          clearTimeout(timer);
          state.aborted = true;
          reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
        });
      });
      const chunks: LanguageModelV1StreamPart[] = [
        { type: 'text-delta', textDelta: `reply to ${lastUserText(prompt)}` },
        { type: 'finish', finishReason: 'stop', usage: { promptTokens: 10, completionTokens: 5 } },
      ];
      return {
        stream: simulateReadableStream({ chunks }),
        rawCall: { rawPrompt: null, rawSettings: {} },
      };
    },
  });
}

function makeRuntime(model: any, config: Partial<AgentConfig> = {}) {
  const storage = new MemoryStorage();
  const bus = new MemoryBus();
  const queue = new MemoryQueue();
  const kv = new MemoryKv();
  const deps: RuntimePorts = {
    storage, bus, queue, kv,
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
    // A short poll keeps these tests fast; production keeps the 500ms default.
    config: resolveConfig({ stopPollMs: 20, ...config }),
  };
  return { deps, runtime: setupAgentCore(deps), storage, bus, queue, kv };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const states = (bus: MemoryBus) =>
  bus.published.filter((e) => e.type === 'STATE_CHANGE').map((e) => (e.payload as any).state);
const texts = (storage: MemoryStorage, threadId: string) =>
  storage.messages.store
    .get(threadId)!
    .filter((m) => m.role === 'assistant')
    .map((m) => (Array.isArray(m.content) ? (m.content as any[]) : [])
      .map((p) => p?.text ?? '')
      .join(''));

describe('stop, then send another message right away (§2.1)', () => {
  it('stops the old run even though the new message overwrote CANCELLED', async () => {
    const state = { aborted: false };
    const { runtime, storage, bus, queue, kv } = makeRuntime(slowModel(state));
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });

    // 1. First message — worker A takes the lock and starts streaming.
    const first = await chat.run({ prompt: 'message A' });
    const threadId = first.threadId;
    const workerA = runtime.worker.handleJob(queue.items[0]!);
    await sleep(30);
    expect(await kv.get(`agent:lock:${threadId}`)).toBe(first.runId!);

    // 2. Stop.
    expect((await chat.stop(threadId)).accepted).toBe(true);
    expect(await kv.get(`agent:state:${threadId}`)).toBe('CANCELLED');

    // 3. A new message lands before worker A's next poll: the state key is
    //    back to RUNNING, so it can no longer carry the stop.
    const second = await chat.run({ threadId, prompt: 'message B' });
    expect(second.runId).not.toBe(first.runId);
    expect(await kv.get(`agent:state:${threadId}`)).toBe('RUNNING');

    // Worker B finds the lock held by the older run and re-dispatches itself
    // instead of dropping the message.
    await runtime.worker.handleJob(queue.items[1]!);
    expect(queue.items).toHaveLength(3);
    expect(queue.items[2]).toMatchObject({ threadId, runId: second.runId });

    // The run id moved, so worker A tears down even with RUNNING on the key.
    await workerA;
    expect(state.aborted).toBe(true);
    expect(await kv.get(`agent:lock:${threadId}`)).toBeNull();

    // The re-dispatched job now runs message B.
    await runtime.worker.handleJob(queue.items[2]!);

    expect(texts(storage, threadId)).toEqual(['reply to message B']);
    expect((await storage.threads.get(threadId))!.state).toBe('COMPLETED');
    // The stopped run wrote no state of its own: no second CANCELLED after
    // the new run's RUNNING.
    expect(states(bus)).toEqual(['RUNNING', 'CANCELLED', 'RUNNING', 'COMPLETED']);
  }, 20_000);

  it('a plain stop with no follow-up message still finalizes CANCELLED', async () => {
    const state = { aborted: false };
    const { runtime, storage, bus, queue } = makeRuntime(slowModel(state));
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });

    const ran = await chat.run({ prompt: 'message A' });
    const workerA = runtime.worker.handleJob(queue.items[0]!);
    await sleep(30);
    await chat.stop(ran.threadId);
    await workerA;

    expect(state.aborted).toBe(true);
    expect((await storage.threads.get(ran.threadId))!.state).toBe('CANCELLED');
    expect(states(bus)).toEqual(['RUNNING', 'CANCELLED', 'CANCELLED']);
  }, 20_000);
});

describe('run identity (§2.1)', () => {
  it('a job whose run id has been replaced does nothing at all', async () => {
    const { runtime, storage, kv } = makeRuntime(slowModel({ aborted: false }));
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });

    const ran = await chat.run({ prompt: 'a' });
    await kv.set(runIdKey(ran.threadId), 'a-newer-run');

    expect(await chat.execute({ threadId: ran.threadId, runId: ran.runId, model: 'gpt-4o' }))
      .toBe('stale');
    // It touched neither the state the newer run owns nor the conversation.
    expect(await kv.get(`agent:state:${ran.threadId}`)).toBe('RUNNING');
    expect(storage.messages.store.get(ran.threadId)!.map((m) => m.role)).toEqual(['user']);
  });

  it('a replaced run cannot write its state over the live one', async () => {
    const { runtime, storage, kv, queue } = makeRuntime(slowModel({ aborted: false }));
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });

    const first = await chat.run({ prompt: 'a' });
    const threadId = first.threadId;
    const workerA = runtime.worker.handleJob(queue.items[0]!);
    await sleep(30);

    // The thread moves on to a newer run while worker A is mid-stream.
    await chat.stop(threadId);
    const second = await chat.run({ threadId, prompt: 'b' });
    await workerA; // finalizes CANCELLED — for a run that no longer owns the thread

    expect(await kv.get(`agent:state:${threadId}`)).toBe('RUNNING');
    expect((await storage.threads.get(threadId))!.state).toBe('RUNNING');
    expect(await kv.get(runIdKey(threadId))).toBe(second.runId!);
    // The tokens it did spend are still billed (§4).
    expect(storage.usage.recorded.length).toBeGreaterThan(0);
  }, 20_000);
});

describe('lock conflicts (§2.8)', () => {
  const lock = (kv: MemoryKv, threadId: string, holder: string) =>
    kv.set(`agent:lock:${threadId}`, holder, { onlyIfNotExists: true, exSeconds: 600 });

  it('re-dispatches a job blocked by an OLDER run instead of dropping it', async () => {
    const { deps, runtime, queue, kv } = makeRuntime(slowModel({ aborted: false }));
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });

    const ran = await chat.run({ prompt: 'a' });
    await lock(kv, ran.threadId, 'an-older-run');

    await chat.executeWithPolicy({ threadId: ran.threadId, runId: ran.runId, model: 'gpt-4o' });

    expect(queue.items).toHaveLength(2);
    expect(queue.items[1]).toMatchObject({
      threadId: ran.threadId,
      runId: ran.runId,
      agent: 'chat',
    });
    expect(queue.delays[1]).toBe(deps.config.runRedriveDelaySeconds);
  });

  it('still drops a duplicate delivery of the run already holding the lock', async () => {
    const { runtime, queue, kv } = makeRuntime(slowModel({ aborted: false }));
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });

    const ran = await chat.run({ prompt: 'a' });
    await lock(kv, ran.threadId, ran.runId!); // the run itself owns the lock

    await chat.executeWithPolicy({ threadId: ran.threadId, runId: ran.runId, model: 'gpt-4o' });

    expect(queue.items).toHaveLength(1); // no redrive — at-least-once no-op
  });

  it('drops a blocked job once a newer run has taken the thread', async () => {
    const { runtime, queue, kv } = makeRuntime(slowModel({ aborted: false }));
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });

    const ran = await chat.run({ prompt: 'a' });
    await lock(kv, ran.threadId, 'an-older-run');
    await kv.set(runIdKey(ran.threadId), 'a-newer-run');

    await chat.executeWithPolicy({ threadId: ran.threadId, runId: ran.runId, model: 'gpt-4o' });

    expect(queue.items).toHaveLength(1);
  });

  it('gives up with FAILED when the lock never clears', async () => {
    const { runtime, storage, queue, kv } = makeRuntime(
      slowModel({ aborted: false }),
      { runMaxAttempts: 2 },
    );
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });

    const ran = await chat.run({ prompt: 'a' });
    await lock(kv, ran.threadId, 'a-wedged-run');
    const job = { threadId: ran.threadId, runId: ran.runId, model: 'gpt-4o' };

    await chat.executeWithPolicy(job);
    await chat.executeWithPolicy(job);
    expect(queue.items).toHaveLength(3); // redriven twice

    await chat.executeWithPolicy(job);
    expect(queue.items).toHaveLength(3); // no third redrive
    expect(await kv.get(`agent:state:${ran.threadId}`)).toBe('FAILED');
    expect((await storage.threads.get(ran.threadId))!.state).toBe('FAILED');
  });
});

describe('a failing provider call (§2.8)', () => {
  /** streamText reports a provider failure as an `error` part and then ends
   *  the stream normally — its text/usage promises never settle. */
  const brokenModel = () =>
    new MockLanguageModelV1({
      provider: 'mock',
      modelId: 'mock-broken',
      doStream: async () => { throw new Error('provider exploded'); },
    });

  it('surfaces the error instead of hanging the worker on an unsettled stream', async () => {
    const { runtime, queue, kv } = makeRuntime(brokenModel());
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const ran = await chat.run({ prompt: 'a' });

    await chat.executeWithPolicy({ threadId: ran.threadId, runId: ran.runId, model: 'gpt-4o' });

    // It came back at all — and it did not walk off with the thread's lock.
    expect(await kv.get(`agent:lock:${ran.threadId}`)).toBeNull();
    // The failure reached the redrive policy rather than vanishing.
    expect(queue.items).toHaveLength(2);
    expect(await kv.get(`agent:attempts:${ran.threadId}`)).toBe('1');
  }, 10_000);
});
