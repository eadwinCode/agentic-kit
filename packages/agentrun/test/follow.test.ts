import { describe, expect, it } from 'bun:test';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { bindStorage } from '../src/core/state.js';
import { sseFrame, SSE_HEADERS } from '../src/core/follow.js';
import { resolveConfig } from '../src/core/types.js';
import type { AgentEvent } from '../src/core/types.js';

async function harness() {
  const storage = new MemoryStorage();
  const bus = new MemoryBus();
  const kv = new MemoryKv();
  const runtime = await setupAgentCore({
    storage, bus, kv,
    queue: new MemoryQueue(),
    admin: new MemoryAdminStore(),
    resolveModel: () => ({ instance: () => ({}) as any, contextWindow: 128_000 }),
    config: resolveConfig(),
  });
  const store = bindStorage(storage, { state: {} });
  const threadId = (await store.threads.create({ model: 'gpt-4o' })).id;

  /** Append to the durable log AND fan out, exactly as publish() does. */
  const publish = async (type: string, payload: unknown = null) => {
    const seq = await kv.incr(`agent:seq:${threadId}`);
    const event: AgentEvent = { threadId, seq, type, payload, createdAt: new Date() };
    await store.events.append(threadId, event);
    await bus.publish(threadId, event);
    return event;
  };
  /** A bus-only notice, like a heartbeat: never persisted, always seq 0. */
  const notice = async (type: string) =>
    bus.publish(threadId, { threadId, seq: 0, type, payload: null, createdAt: new Date() } as AgentEvent);

  return { runtime, threadId, publish, notice, bus };
}

/** Take n events, then stop — which also closes the generator. */
async function take(gen: AsyncGenerator<AgentEvent>, n: number) {
  const out: AgentEvent[] = [];
  if (n === 0) return out;
  for await (const e of gen) {
    out.push(e);
    if (out.length >= n) break;
  }
  return out;
}

describe('events.follow (§2.2)', () => {
  it('replays the durable log, then goes live, in order', async () => {
    const h = await harness();
    await h.publish('A');
    await h.publish('B');

    const gen = h.runtime.events.follow(h.threadId);
    const replayed = await take(gen, 2);
    expect(replayed.map((e) => e.type)).toEqual(['A', 'B']);
  });

  it('resumes after a cursor and never re-sends what the client has', async () => {
    const h = await harness();
    const a = await h.publish('A');
    await h.publish('B');
    await h.publish('C');

    const gen = h.runtime.events.follow(h.threadId, { since: a.seq });
    expect((await take(gen, 2)).map((e) => e.type)).toEqual(['B', 'C']);
  });

  // The reason to subscribe before replaying: an event published in the gap
  // between the two would otherwise be lost for ever.
  it('does not drop an event published during the replay', async () => {
    const h = await harness();
    await h.publish('A');

    const gen = h.runtime.events.follow(h.threadId);
    // Start the generator: it subscribes, then reads the durable log.
    const first = await gen.next();
    expect((first.value as AgentEvent).type).toBe('A');

    await h.publish('B');
    const second = await gen.next();
    expect((second.value as AgentEvent).type).toBe('B');
    await gen.return(undefined as never);
  });

  it('forwards a bus-only notice without moving the cursor', async () => {
    const h = await harness();
    const gen = h.runtime.events.follow(h.threadId);
    const collected: AgentEvent[] = [];
    const reader = (async () => {
      for await (const e of gen) {
        collected.push(e);
        if (collected.length >= 2) break;
      }
    })();

    await new Promise((r) => setTimeout(r, 10));
    await h.notice('HEARTBEAT');
    await h.publish('A');
    await reader;

    expect(collected.map((e) => e.type)).toEqual(['HEARTBEAT', 'A']);
    // The notice did not consume a seq, so 'A' still arrived.
    expect(collected[1]!.seq).toBeGreaterThan(0);
  });

  it('unsubscribes when the consumer stops', async () => {
    const h = await harness();
    await h.publish('A');
    const gen = h.runtime.events.follow(h.threadId);
    await take(gen, 1);
    await gen.return(undefined as never);
    expect(h.bus.subscribers(h.threadId)).toBe(0);
  });

  it('unsubscribes when the request aborts', async () => {
    const h = await harness();
    const controller = new AbortController();
    const gen = h.runtime.events.follow(h.threadId, { signal: controller.signal });

    const done = (async () => {
      for await (const _ of gen) { /* wait for live events */ }
    })();
    await new Promise((r) => setTimeout(r, 10));
    controller.abort();
    await done;

    expect(h.bus.subscribers(h.threadId)).toBe(0);
  });
});

describe('events.sse (§2.2)', () => {
  const decode = (chunk: Uint8Array) => new TextDecoder().decode(chunk);

  it('encodes frames with the seq as the id', async () => {
    const h = await harness();
    await h.publish('A', { x: 1 });

    const { stream, headers } = h.runtime.events.sse(h.threadId);
    expect(headers['Content-Type']).toContain('text/event-stream');

    const reader = stream.getReader();
    const first = decode((await reader.read()).value!);
    expect(first).toMatch(/^id: \d+\ndata: \{/);
    expect(first.endsWith('\n\n')).toBe(true);
    await reader.cancel();
  });

  // EventSource stores any id it sees and sends it back as Last-Event-ID. An
  // `id: 0` on a heartbeat would rewind the cursor to the start of the thread.
  it('omits the id on a bus-only notice', () => {
    const notice = { threadId: 't', seq: 0, type: 'HEARTBEAT', payload: null, createdAt: new Date() };
    expect(sseFrame(notice as AgentEvent)).not.toContain('id:');
    const real = { ...notice, seq: 7, type: 'A' };
    expect(sseFrame(real as AgentEvent)).toContain('id: 7\n');
  });

  it('emits a retry hint first when asked', async () => {
    const h = await harness();
    await h.publish('A');
    const { stream } = h.runtime.events.sse(h.threadId, { retryMs: 3000 });
    const reader = stream.getReader();
    expect(decode((await reader.read()).value!)).toBe('retry: 3000\n\n');
    await reader.cancel();
  });

  it('unsubscribes when the client hangs up', async () => {
    const h = await harness();
    await h.publish('A');
    const { stream } = h.runtime.events.sse(h.threadId);
    const reader = stream.getReader();
    await reader.read();
    await reader.cancel();
    await new Promise((r) => setTimeout(r, 10));
    expect(h.bus.subscribers(h.threadId)).toBe(0);
  });

  it('sets the headers a proxy needs to leave the stream alone', () => {
    expect(SSE_HEADERS['Cache-Control']).toContain('no-transform');
    expect(SSE_HEADERS['X-Accel-Buffering']).toBe('no');
  });
});
