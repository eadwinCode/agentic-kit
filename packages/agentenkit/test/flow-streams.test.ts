import { describe, expect, it } from 'bun:test';
import { Flow } from '@clevix/flow';
import { FlowBus, FlowStreams, flowStreamId, flowThreadChannel } from '../src/adapters/flow.js';
import { MemoryKv } from '../src/adapters/memory.js';
import type { AgentEvent } from '../src/core/types.js';
import type { StreamEvent } from '../src/core/stream-events.js';
import { runStreamsSuite } from './run-streams-suite.js';

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe('flow names', () => {
  it('keeps a name Flow allows, and hashes any other', async () => {
    expect(await flowStreamId('run_1:2')).toBe('agentenkit.run_1:2');
    expect(await flowThreadChannel('t-1')).toBe('agentenkit.thread.t-1');
    expect(await flowThreadChannel('a b/c')).toMatch(/^agentenkit\.thread\.h\.[0-9a-f]{64}$/);
    expect(await flowStreamId('x'.repeat(300))).toMatch(/^agentenkit\.h\.[0-9a-f]{64}$/);
  });
});

// Against a real engine: set TEST_FLOW_BASE_URL, TEST_FLOW_APP_ID and
// TEST_FLOW_SIGNING_KEY (an app's signing key). With none, these skip.
const baseUrl = process.env.TEST_FLOW_BASE_URL;
const signingKey = process.env.TEST_FLOW_SIGNING_KEY;
const appId = process.env.TEST_FLOW_APP_ID;
const live = baseUrl && signingKey && appId;
if (!live) console.warn('flow-streams: TEST_FLOW_BASE_URL, TEST_FLOW_APP_ID or TEST_FLOW_SIGNING_KEY not set; the engine tests skip');
const flow = new Flow({ id: appId ?? 'app', baseUrl: baseUrl ?? 'http://127.0.0.1:1', signingKey: signingKey ?? '' });

if (live) {
  const kv = new MemoryKv();
  runStreamsSuite('flow', async () => new FlowStreams(flow, { kv }), { other: async () => new FlowStreams(flow, { kv }), settleMs: 500 });
}

(live ? describe : describe.skip)('flow streams and bus, on the engine', () => {
  it('an item over 60 KB goes through the kv and comes back whole', async () => {
    const kv = new MemoryKv();
    const s = new FlowStreams(flow, { kv });
    const id = `big-${Date.now()}:1`;
    await s.open(id, { threadId: 't', runId: 'r' }, 60_000);
    const big: StreamEvent = { type: 'TEXT_MESSAGE_CONTENT', messageId: 'm', delta: 'x'.repeat(100_000) };
    const small: StreamEvent = { type: 'TEXT_MESSAGE_CONTENT', messageId: 'm', delta: 'y' };
    const offsets = await s.append(id, [small, big, small]);
    expect(offsets).toHaveLength(3);
    const snap = await s.snapshot(id);
    expect(snap!.items.map(({ offset: _, ...e }) => e)).toEqual([small, big, small]);
    await s.delete(id);
  });

  it('an append of more than 100 items goes in batches, in order', async () => {
    const s = new FlowStreams(flow, { kv: new MemoryKv() });
    const id = `many-${Date.now()}:1`;
    await s.open(id, { threadId: 't', runId: 'r' }, 60_000);
    const events = Array.from({ length: 250 }, (_, i): StreamEvent => ({ type: 'TEXT_MESSAGE_CONTENT', messageId: 'm', delta: String(i) }));
    expect(await s.append(id, events)).toHaveLength(250);
    const snap = await s.snapshot(id);
    expect(snap!.items.map((i) => (i as { delta: string }).delta)).toEqual(events.map((e) => (e as { delta: string }).delta));
    await s.delete(id);
  });

  it('the bus delivers to every subscriber, big events too, until they leave', async () => {
    const kv = new MemoryKv();
    const bus = new FlowBus(flow, { kv });
    const threadId = `bus-${Date.now()}`;
    const a: AgentEvent[] = [];
    const b: AgentEvent[] = [];
    const offA = await bus.subscribe(threadId, (e) => a.push(e));
    const offB = await bus.subscribe(threadId, (e) => b.push(e));
    const event = (seq: number, text: string) =>
      ({ threadId, seq, type: 'STATE_CHANGE', payload: { text }, createdAt: new Date() }) as unknown as AgentEvent;
    await bus.publish(threadId, event(1, 'small'));
    await bus.publish(threadId, event(2, 'z'.repeat(100_000)));
    for (let i = 0; i < 50 && (a.length < 2 || b.length < 2); i++) await sleep(100);
    expect(a.map((e) => e.seq)).toEqual([1, 2]);
    expect(b.map((e) => e.seq)).toEqual([1, 2]);
    expect(a[0]!.createdAt).toBeInstanceOf(Date);
    expect((a[1]!.payload as { text: string }).text).toHaveLength(100_000);
    offA();
    await bus.publish(threadId, event(3, 'after'));
    for (let i = 0; i < 50 && b.length < 3; i++) await sleep(100);
    expect(a.map((e) => e.seq)).toEqual([1, 2]);
    expect(b.map((e) => e.seq)).toEqual([1, 2, 3]);
    offB();
  });
});
