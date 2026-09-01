import { describe, expect, it } from 'bun:test';
import { simulateReadableStream } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { resolveConfig } from '../src/core/types.js';
import type { RuntimePorts } from '../src/ports/runtime.js';

/** Answers whichever user turn it was last given, so the tests can tell an
 *  edited conversation apart from the original. */
function echoModel() {
  return new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock-echo',
    doStream: async ({ prompt }: any) => {
      const users = (prompt ?? []).filter((m: any) => m.role === 'user');
      const c = users.at(-1)?.content;
      const text = typeof c === 'string' ? c : (c ?? []).map((p: any) => p?.text ?? '').join('');
      return {
        stream: simulateReadableStream({
          chunks: [
            { type: 'text-delta', textDelta: `reply to ${text}` },
            { type: 'finish', finishReason: 'stop', usage: { promptTokens: 10, completionTokens: 5 } },
          ] as LanguageModelV1StreamPart[],
        }),
        rawCall: { rawPrompt: null, rawSettings: {} },
      };
    },
  });
}

function makeRuntime() {
  const storage = new MemoryStorage();
  const bus = new MemoryBus();
  const queue = new MemoryQueue();
  const kv = new MemoryKv();
  const deps: RuntimePorts = {
    storage, bus, queue, kv,
    resolveModel: () => ({ instance: () => echoModel(), contextWindow: 128_000 }),
    config: resolveConfig(),
  };
  return { deps, runtime: setupAgentCore(deps), storage, queue, kv };
}

const shape = (storage: MemoryStorage, threadId: string) =>
  storage.messages.store.get(threadId)!.map((m) => {
    const c: any = m.content;
    const text = typeof c === 'string' ? c : (Array.isArray(c) ? c : []).map((p) => p?.text ?? '').join('');
    return `${m.role}: ${text}`;
  });

describe('edit + resend (§5.1)', () => {
  it('replaces the edited turn and everything it led to, then answers again', async () => {
    const r = makeRuntime();
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });

    const first = await chat.run({ prompt: 'question one' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);
    const second = await chat.run({ threadId: first.threadId, prompt: 'question two' });
    await r.runtime.worker.handleJob(r.queue.items[1]!);

    expect(shape(r.storage, first.threadId)).toEqual([
      'user: question one',
      'assistant: reply to question one',
      'user: question two',
      'assistant: reply to question two',
    ]);

    // Edit the FIRST question: its answer and the whole second turn go with it.
    const target = r.storage.messages.store.get(first.threadId)![0]!;
    const edited = await chat.run({
      threadId: first.threadId,
      prompt: 'question one, reworded',
      editMessageId: target.id,
    });
    expect(edited.accepted).toBe(true);
    expect(shape(r.storage, first.threadId)).toEqual(['user: question one, reworded']);

    await r.runtime.worker.handleJob(r.queue.items.at(-1)!);
    expect(shape(r.storage, first.threadId)).toEqual([
      'user: question one, reworded',
      'assistant: reply to question one, reworded',
    ]);
    expect(second.threadId).toBe(first.threadId); // one thread, no fork
  });

  it('is a fresh run: it claims a new run id', async () => {
    const r = makeRuntime();
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });

    const first = await chat.run({ prompt: 'a' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);
    const target = r.storage.messages.store.get(first.threadId)![0]!;

    const edited = await chat.run({
      threadId: first.threadId, prompt: 'b', editMessageId: target.id,
    });
    expect(edited.runId).not.toBe(first.runId);
    expect(r.queue.items.at(-1)).toMatchObject({ runId: edited.runId });
  });

  it('refuses to edit anything but a user turn', async () => {
    const r = makeRuntime();
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });

    const first = await chat.run({ prompt: 'a' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);
    const assistant = r.storage.messages.store.get(first.threadId)![1]!;
    const before = shape(r.storage, first.threadId);

    // Cutting from an assistant turn could orphan a tool result from the
    // tool-call that produced it.
    const res = await chat.run({
      threadId: first.threadId, prompt: 'nope', editMessageId: assistant.id,
    });
    expect(res).toMatchObject({ accepted: false, error: 'Only a user message can be edited' });
    expect(shape(r.storage, first.threadId)).toEqual(before); // nothing touched
  });

  it('refuses an unknown message id, leaving the thread alone', async () => {
    const r = makeRuntime();
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const first = await chat.run({ prompt: 'a' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);
    const before = shape(r.storage, first.threadId);

    const res = await chat.run({
      threadId: first.threadId, prompt: 'x', editMessageId: 'not-a-real-id',
    });
    expect(res).toMatchObject({ accepted: false, error: 'Message not found' });
    expect(shape(r.storage, first.threadId)).toEqual(before);
  });

  it('is refused outright while a run is live', async () => {
    const r = makeRuntime();
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const first = await chat.run({ prompt: 'a' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);
    const target = r.storage.messages.store.get(first.threadId)![0]!;

    await r.kv.set(`agent:state:${first.threadId}`, 'RUNNING'); // a run is in flight
    const res = await chat.run({
      threadId: first.threadId, prompt: 'b', editMessageId: target.id,
    });
    expect(res).toMatchObject({ accepted: false, error: 'Thread has an active run' });
    expect(shape(r.storage, first.threadId)).toHaveLength(2); // history intact
  });
});

describe('messages.deleteFrom', () => {
  it('drops the message and its suffix, and reports the count', async () => {
    const storage = new MemoryStorage();
    const t = (await storage.threads.create({ model: 'gpt-4o' })).id;
    const a = await storage.messages.append(t, { role: 'user', content: 'a' });
    const b = await storage.messages.append(t, { role: 'assistant', content: 'b' });
    await storage.messages.append(t, { role: 'user', content: 'c' });

    expect(await storage.messages.deleteFrom(t, b.id)).toBe(2);
    expect((await storage.messages.list(t)).map((m) => m.id)).toEqual([a.id]);
    // An id from another thread (or a gone one) removes nothing.
    expect(await storage.messages.deleteFrom(t, b.id)).toBe(0);
  });
});
