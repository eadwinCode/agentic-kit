import { describe, expect, it } from 'bun:test';
import { simulateReadableStream, tool } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { z } from 'zod';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { bindStorage } from '../src/core/state.js';
import { followEvents } from '../src/core/follow.js';
import { publish } from '../src/core/publish.js';
import { resolveConfig } from '../src/core/types.js';
import type { AgentEvent } from '../src/core/types.js';
import type { RuntimePorts } from '../src/ports/runtime.js';

// Workstream G: a follower never skips a seq, a lost seq counter carries on
// from the log, token deltas go out merged, and the platform's own event
// types stay reserved. The same cases run in the Go package (events_test.go).

/** One step: stream these parts, then finish. */
function model(steps: LanguageModelV1StreamPart[][]) {
  let call = 0;
  return new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock',
    doStream: async () => {
      const parts = steps[Math.min(call++, steps.length - 1)]!;
      return { stream: simulateReadableStream({ chunks: parts }), rawCall: { rawPrompt: null, rawSettings: {} } };
    },
  });
}
const finish = (reason = 'stop') =>
  ({ type: 'finish', finishReason: reason, usage: { promptTokens: 10, completionTokens: 5 } }) as LanguageModelV1StreamPart;
const say = (text: string) => ({ type: 'text-delta', textDelta: text }) as LanguageModelV1StreamPart;

async function makeRuntime(m: any, tools: Record<string, any> = {}) {
  const storage = new MemoryStorage();
  const bus = new MemoryBus();
  const queue = new MemoryQueue();
  const kv = new MemoryKv();
  const admin = new MemoryAdminStore();
  const config = resolveConfig({ stopPollMs: 20 });
  const resolveModel = () => ({ instance: () => m, contextWindow: 128_000 });
  const runtime = await setupAgentCore({ storage, bus, queue, kv, admin, config, resolveModel });
  const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o', tools });
  const ports: RuntimePorts = { storage: bindStorage(storage, { state: {} }), bus, queue, kv, admin, config, resolveModel };
  const events = (threadId: string, type: string) => bus.published.filter((e) => e.threadId === threadId && e.type === type);
  return { runtime, chat, storage, bus, queue, kv, ports, events };
}

describe('events (§2.2)', () => {
  it('an event the bus skipped is read back from the log', async () => {
    const r = await makeRuntime(model([[say('x'), finish()]]));
    const th = await r.storage.threads.create({});
    const store = async (seq: number) => {
      const e = { threadId: th.id, seq, type: 'X', payload: null, createdAt: new Date() } as AgentEvent;
      await r.storage.events.append(th.id, e);
      return e;
    };
    await store(1);
    const stop = new AbortController();
    const stream = followEvents(r.ports, th.id, { signal: stop.signal });
    expect((await stream.next()).value!.seq).toBe(1); // the replay
    // Seq 2 is stored but its bus message is lost; seq 3 arrives live.
    await store(2);
    const e3 = await store(3);
    const second = stream.next();
    await new Promise((resolve) => setTimeout(resolve, 10));
    await r.bus.publish(th.id, e3);
    expect((await second).value!.seq).toBe(2); // the gap is read back first
    expect((await stream.next()).value!.seq).toBe(3); // then the live event
    stop.abort();
    await stream.return(undefined);
  });

  it('a lost seq counter carries on from the log', async () => {
    const r = await makeRuntime(model([[say('x'), finish()]]));
    const th = await r.storage.threads.create({});
    for (let i = 0; i < 3; i++) await publish(r.ports, th.id, 'X', null);
    await r.kv.del(`agent:seq:${th.id}`); // flushed, evicted, restarted
    expect((await publish(r.ports, th.id, 'X', null)).seq).toBe(4); // past what the log holds
  });

  it('consecutive deltas go out as one event', async () => {
    const r = await makeRuntime(model([[say('one '), say('two '), say('three'), finish()]]));
    const ran = await r.chat.run({ prompt: 'go' });
    await r.runtime.worker.handleJob(r.queue.items.shift()!);
    const deltas = r.events(ran.threadId, 'CHUNK')
      .map((e) => e.payload as any)
      .filter((p) => p.type === 'text-delta')
      .map((p) => p.textDelta);
    expect(deltas.join('')).toBe('one two three'); // the text arrives whole
    expect(deltas).toHaveLength(1); // as one event
  });

  it('an app cannot publish the cost budget event', async () => {
    const r = await makeRuntime(model([[say('x'), finish()]]));
    const th = await r.storage.threads.create({});
    await expect(r.runtime.events.publishEvent(th.id, 'COST_BUDGET_EXHAUSTED', null)).rejects.toThrow(
      'platform event type',
    );
  });

  it("a failed tool's chunk names its error", async () => {
    const r = await makeRuntime(
      model([
        [
          { type: 'tool-call', toolCallType: 'function', toolCallId: 'c1', toolName: 'lookup', args: '{}' },
          { type: 'tool-call', toolCallType: 'function', toolCallId: 'c2', toolName: 'broken', args: '{}' },
          finish('tool-calls'),
        ],
        [say('done'), finish()],
      ]),
      {
        lookup: tool({ parameters: z.object({}), execute: async () => ({ found: true }) }),
        broken: tool({ parameters: z.object({}), execute: async () => { throw new Error('boom'); } }),
      },
    );
    const ran = await r.chat.run({ prompt: 'go' });
    await r.runtime.worker.handleJob(r.queue.items.shift()!);
    const results = new Map(
      r.events(ran.threadId, 'CHUNK')
        .map((e) => e.payload as any)
        .filter((p) => p.type === 'tool-result')
        .map((p) => [p.toolCallId, p]),
    );
    expect(results.get('c1')).toEqual({ type: 'tool-result', toolCallId: 'c1', toolName: 'lookup', result: { found: true } });
    expect(results.get('c2').result).toEqual({ error: 'boom' }); // a failed tool names its error
  });
});
