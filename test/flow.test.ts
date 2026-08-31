import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { createAgentRuntime } from '../src/runtime.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import type { AgentConfig } from '../src/core/types.js';
import { resolveConfig } from '../src/core/types.js';
import type { RuntimePorts } from '../src/ports/runtime.js';

function makeDeps(config: Partial<AgentConfig> = {}) {
  const storage = new MemoryStorage();
  const bus = new MemoryBus();
  const queue = new MemoryQueue();
  const kv = new MemoryKv();
  const deps: RuntimePorts = { storage, bus, queue, kv, models: {}, config: resolveConfig(config) };
  return { deps, runtime: createAgentRuntime(deps), storage, bus, queue, kv };
}

/** Simulate a thread parked mid-HITL (§2.5): WAITING_FOR_INPUT + pending request. */
async function parkThread(deps: RuntimePorts, toolCallId = 'call_1') {
  const thread = await deps.storage.threads.create();
  const seq = await deps.kv.incr(`agent:seq:${thread.id}`);
  await deps.storage.threads.setState(thread.id, 'WAITING_FOR_INPUT');
  await deps.kv.set(`agent:state:${thread.id}`, 'WAITING_FOR_INPUT');
  await deps.storage.events.append(thread.id, {
    threadId: thread.id,
    seq,
    type: 'INPUT_REQUIRED',
    payload: { toolCallId, toolName: 'sendEmail', arguments: { to: 'a@b.c' } },
    createdAt: new Date(),
  });
  return thread.id;
}

describe('runtime.run (§5.1)', () => {
  it('creates a thread, persists the user message, marks RUNNING, enqueues', async () => {
    const { deps, runtime, queue } = makeDeps();
    const res = await runtime.run({ prompt: 'hello', model: 'gpt-4o' });

    assert.equal(res.accepted, true);
    assert.equal(res.state, 'RUNNING');
    const thread = await deps.storage.threads.get(res.threadId);
    assert.equal(thread!.state, 'RUNNING');
    const messages = await deps.storage.messages.list(res.threadId);
    assert.equal(messages.length, 1);
    assert.equal(messages[0].role, 'user');
    assert.equal(queue.items.length, 1);
    assert.equal(queue.items[0].threadId, res.threadId);
  });

  it('rejects a second run while one is active (§2.1 guard)', async () => {
    const { runtime } = makeDeps();
    const first = await runtime.run({ prompt: 'a', model: 'gpt-4o' });
    const second = await runtime.run({ threadId: first.threadId, prompt: 'b', model: 'gpt-4o' });
    assert.equal(second.accepted, false);
    assert.match(second.error!, /active run/);
  });

  it('rejects when the billing pre-check fails (§4)', async () => {
    const { runtime } = makeDeps({
      billingPreCheck: async () => ({ ok: false, error: 'Insufficient credits' }),
    });
    const res = await runtime.run({ prompt: 'x', model: 'gpt-4o' });
    assert.equal(res.accepted, false);
    assert.match(res.error!, /Insufficient credits/);
  });
});

describe('runtime.stop (§5.2)', () => {
  it('writes CANCELLED to cache + durable truth and publishes STATE_CHANGE', async () => {
    const { deps, runtime, bus } = makeDeps();
    const ran = await runtime.run({ prompt: 'a', model: 'gpt-4o' });

    const res = await runtime.stop(ran.threadId);
    assert.equal(res.accepted, true);
    assert.equal(await deps.kv.get(`agent:state:${ran.threadId}`), 'CANCELLED');
    const thread = await deps.storage.threads.get(ran.threadId);
    assert.equal(thread!.state, 'CANCELLED');
    const last = bus.published.at(-1)!;
    assert.equal(last.type, 'STATE_CHANGE');
    assert.equal((last.payload as any).state, 'CANCELLED');
  });

  it('rejects stopping an IDLE thread', async () => {
    const { deps, runtime } = makeDeps();
    const thread = await deps.storage.threads.create();
    const res = await runtime.stop(thread.id);
    assert.equal(res.accepted, false);
    assert.match(res.error!, /IDLE/);
  });
});

describe('runtime.hitl.respond (§5.4)', () => {
  it('delivers an approval to the handoff key when parked (§2.5)', async () => {
    const { deps, runtime } = makeDeps();
    const threadId = await parkThread(deps);

    const res = await runtime.hitl.respond({
      threadId, toolCallId: 'call_1', approved: true, payload: { ok: 1 },
    });
    assert.equal(res.delivered, true);
    const raw = await deps.kv.get('agent:hitl:call_1');
    assert.deepEqual(JSON.parse(raw!), { approved: true, payload: { ok: 1 } });
  });

  it('rejects an unknown toolCallId (answer-latest policy, §2.7)', async () => {
    const { deps, runtime } = makeDeps();
    const threadId = await parkThread(deps, 'call_latest');
    const res = await runtime.hitl.respond({
      threadId, toolCallId: 'call_older', approved: true,
    });
    assert.equal(res.delivered, false);
    assert.match(res.error!, /No matching/);
  });

  it('rejects a response for a thread that is not WAITING_FOR_INPUT', async () => {
    const { deps, runtime } = makeDeps();
    const thread = await deps.storage.threads.create();
    const res = await runtime.hitl.respond({
      threadId: thread.id, toolCallId: 'call_1', approved: true,
    });
    assert.equal(res.delivered, false);
  });
});

describe('reclaimIfOrphaned (§2.5)', () => {
  it('does nothing for a young pending request', async () => {
    const { deps, runtime } = makeDeps({ hitlTtlMs: 5, reclaimGraceMs: 1 });
    const thread = await deps.storage.threads.create();
    const seq = await deps.kv.incr(`agent:seq:${thread.id}`);
    await deps.storage.threads.setState(thread.id, 'WAITING_FOR_INPUT');
    await deps.storage.events.append(thread.id, {
      threadId: thread.id, seq, type: 'INPUT_REQUIRED', payload: { toolCallId: 'c1' }, createdAt: new Date(),
    });

    assert.equal(await runtime.hitl.reclaimIfOrphaned(thread.id), false);
  });

  it('claims a true orphan exactly once: appends timeout result, re-enqueues (§2.8)', async () => {
    const { deps, runtime, queue, bus } = makeDeps({ hitlTtlMs: 5, reclaimGraceMs: 1 });
    const thread = await deps.storage.threads.create();
    const seq = await deps.kv.incr(`agent:seq:${thread.id}`);
    await deps.storage.threads.setState(thread.id, 'WAITING_FOR_INPUT');
    await deps.kv.set(`agent:state:${thread.id}`, 'WAITING_FOR_INPUT');
    await deps.storage.events.append(thread.id, {
      threadId: thread.id, seq, type: 'INPUT_REQUIRED', payload: { toolCallId: 'c1' },
      createdAt: new Date(Date.now() - 60_000), // far older than TTL + grace
    });

    assert.equal(await runtime.hitl.reclaimIfOrphaned(thread.id), true);
    // Second caller loses the CAS race (§3.4)
    assert.equal(await runtime.hitl.reclaimIfOrphaned(thread.id), false);

    const threadAfter = await deps.storage.threads.get(thread.id);
    assert.equal(threadAfter!.state, 'RUNNING');
    const messages = await deps.storage.messages.list(thread.id);
    assert.equal(messages.length, 1);
    assert.deepEqual((messages[0].content as any).result, {
      responded: false, cancelled: true, reason: 'timeout',
    });
    assert.equal(queue.items.length, 1); // re-entered via the queue
    const last = bus.published.at(-1)!;
    assert.equal(last.type, 'STATE_CHANGE');
  });
});

describe('runtime.events (§2.2)', () => {
  it('replays since a cursor and subscribes live', async () => {
    const { deps, runtime } = makeDeps();
    const thread = await deps.storage.threads.create();
    for (let i = 1; i <= 3; i++) {
      await deps.storage.events.append(thread.id, {
        threadId: thread.id, seq: i, type: 'CHUNK', payload: { i }, createdAt: new Date(),
      });
    }

    const replay = await runtime.events.since(thread.id, 1);
    assert.deepEqual(replay.map((e) => e.seq), [2, 3]);

    const seen: number[] = [];
    const unsubscribe = await runtime.events.subscribe(thread.id, (e) => seen.push(e.seq));
    await deps.bus.publish(thread.id, {
      threadId: thread.id, seq: 4, type: 'CHUNK', payload: {}, createdAt: new Date(),
    });
    assert.deepEqual(seen, [4]);
    unsubscribe();
  });
});

describe('contextBudget (§2.6)', () => {
  it('caps everything at the 265k ceiling but keeps smaller native windows', async () => {
    const { runtime } = makeDeps();
    const deps = (runtime as any).deps ?? null;
    // via public behavior instead: compactContext is core-internal, so assert budget math
    const { contextBudget } = await import('../src/core/context.js');
    // modelRegistry empty here; budgets come from config/native windows
    const deps2 = makeDeps().deps;
    assert.equal(contextBudget(deps2, 'gpt-4o'), 128_000);
    assert.equal(contextBudget(deps2, 'claude-3-5-sonnet'), 200_000);
    assert.equal(contextBudget(deps2, 'gemini-1.5-pro'), 265_000);
    assert.equal(contextBudget(deps2, 'unknown-model'), 265_000);
    void deps;
  });
});
