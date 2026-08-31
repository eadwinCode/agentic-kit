import { describe, expect, it } from 'bun:test';
import { createAgentRuntime } from '../src/runtime.js';
import { contextBudget } from '../src/core/context.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import type { AgentConfig } from '../src/core/types.js';
import { resolveConfig } from '../src/core/types.js';
import type { RuntimePorts } from '../src/ports/runtime.js';

const MODEL_WINDOWS: Record<string, number> = {
  'gpt-4o': 128_000,
  'claude-3-5-sonnet': 200_000,
  'gemini-1.5-pro': 265_000,
};

function makeDeps(config: Partial<AgentConfig> = {}) {
  const storage = new MemoryStorage();
  const bus = new MemoryBus();
  const queue = new MemoryQueue();
  const kv = new MemoryKv();
  const deps: RuntimePorts = {
    storage,
    bus,
    queue,
    kv,
    resolveModel: (name) => ({
      instance: () => {
        throw new Error('no-llm');
      },
      contextWindow: MODEL_WINDOWS[name] ?? 128_000,
    }),
    config: resolveConfig(config),
  };
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

describe('runtime.run via handle (§5.1)', () => {
  it('creates a thread, persists the user message, marks RUNNING, enqueues', async () => {
    const { deps, runtime, queue } = makeDeps();
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });

    const res = await chat.run({ prompt: 'hello' });

    expect(res.accepted).toBe(true);
    expect(res.state).toBe('RUNNING');
    const thread = await deps.storage.threads.get(res.threadId);
    expect(thread!.state).toBe('RUNNING');
    expect(thread!.model).toBe('gpt-4o');
    const messages = await deps.storage.messages.list(res.threadId);
    expect(messages).toHaveLength(1);
    expect(messages[0]!.role).toBe('user');
    expect(queue.items).toHaveLength(1);
    expect(queue.items[0]!.threadId).toBe(res.threadId);
    expect(queue.items[0]!.agent).toBe('chat');
  });

  it('rejects a second run while one is active (§2.1 guard)', async () => {
    const { runtime } = makeDeps();
    const chat = runtime.createStreamTextAgent({ name: 'chat' });
    const first = await chat.run({ prompt: 'a' });
    const second = await chat.run({ threadId: first.threadId, prompt: 'b' });
    expect(second.accepted).toBe(false);
    expect(second.error).toMatch(/active run/);
  });

  it('rejects when the billing pre-check fails (§4)', async () => {
    const { runtime } = makeDeps({
      billingPreCheck: async () => ({ ok: false, error: 'Insufficient credits' }),
    });
    const chat = runtime.createStreamTextAgent({ name: 'chat' });
    const res = await chat.run({ prompt: 'x' });
    expect(res.accepted).toBe(false);
    expect(res.error).toMatch(/Insufficient credits/);
  });

  it('threads tokenBudget through the job (§2.1)', async () => {
    const { deps, runtime, queue } = makeDeps();
    const chat = runtime.createStreamTextAgent({ name: 'chat' });
    await chat.run({ prompt: 'a', tokenBudget: 4_000 });
    expect(queue.items[0]!.tokenBudget).toBe(4_000);
  });

  it('lists threads most recent first (thread picker)', async () => {
    const { deps, runtime } = makeDeps();
    const first = await deps.storage.threads.create();
    await new Promise((r) => setTimeout(r, 5)); // ensure distinct updatedAt
    const second = await deps.storage.threads.create();

    const threads = await runtime.listThreads();
    expect(threads.map((t) => t.id)).toEqual([second.id, first.id]);
  });
});

describe('runtime.stop (§5.2)', () => {
  it('writes CANCELLED to cache + durable truth and publishes STATE_CHANGE', async () => {
    const { deps, runtime, bus } = makeDeps();
    const chat = runtime.createStreamTextAgent({ name: 'chat' });
    const ran = await chat.run({ prompt: 'a' });

    const res = await chat.stop(ran.threadId);
    expect(res.accepted).toBe(true);
    expect(await deps.kv.get(`agent:state:${ran.threadId}`)).toBe('CANCELLED');
    const thread = await deps.storage.threads.get(ran.threadId);
    expect(thread!.state).toBe('CANCELLED');
    const last = bus.published.at(-1)!;
    expect(last.type).toBe('STATE_CHANGE');
    expect((last.payload as any).state).toBe('CANCELLED');
  });

  it('rejects stopping an IDLE thread', async () => {
    const { deps, runtime } = makeDeps();
    const chat = runtime.createStreamTextAgent({ name: 'chat' });
    const thread = await deps.storage.threads.create();
    const res = await chat.stop(thread.id);
    expect(res.accepted).toBe(false);
    expect(res.error).toMatch(/IDLE/);
  });
});

describe('runtime.hitl.respond (§5.4)', () => {
  it('delivers an approval to the handoff key when parked (§2.5)', async () => {
    const { deps, runtime } = makeDeps();
    const threadId = await parkThread(deps);

    const res = await runtime.hitl.respond({
      threadId, toolCallId: 'call_1', approved: true, payload: { ok: 1 },
    });
    expect(res.delivered).toBe(true);
    const raw = await deps.kv.get('agent:hitl:call_1');
    expect(JSON.parse(raw!)).toEqual({ approved: true, payload: { ok: 1 } });
  });

  it('rejects an unknown toolCallId (answer-latest policy, §2.7)', async () => {
    const { deps, runtime } = makeDeps();
    const threadId = await parkThread(deps, 'call_latest');
    const res = await runtime.hitl.respond({
      threadId, toolCallId: 'call_older', approved: true,
    });
    expect(res.delivered).toBe(false);
    expect(res.error).toMatch(/No matching/);
  });

  it('rejects a response for a thread that is not WAITING_FOR_INPUT', async () => {
    const { deps, runtime } = makeDeps();
    const thread = await deps.storage.threads.create();
    const res = await runtime.hitl.respond({
      threadId: thread.id, toolCallId: 'call_1', approved: true,
    });
    expect(res.delivered).toBe(false);
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

    expect(await runtime.hitl.reclaimIfOrphaned(thread.id)).toBe(false);
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

    expect(await runtime.hitl.reclaimIfOrphaned(thread.id)).toBe(true);
    // Second caller loses the CAS race (§3.4)
    expect(await runtime.hitl.reclaimIfOrphaned(thread.id)).toBe(false);

    const threadAfter = await deps.storage.threads.get(thread.id);
    expect(threadAfter!.state).toBe('RUNNING');
    const messages = await deps.storage.messages.list(thread.id);
    expect(messages).toHaveLength(1);
    expect((messages[0]!.content as any).result).toEqual({
      responded: false, cancelled: true, reason: 'timeout',
    });
    expect(queue.items).toHaveLength(1); // re-entered via the queue
    const last = bus.published.at(-1)!;
    expect(last.type).toBe('STATE_CHANGE');
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
    expect(replay.map((e) => e.seq)).toEqual([2, 3]);

    const seen: number[] = [];
    const unsubscribe = await runtime.events.subscribe(thread.id, (e) => seen.push(e.seq));
    await deps.bus.publish(thread.id, {
      threadId: thread.id, seq: 4, type: 'CHUNK', payload: {}, createdAt: new Date(),
    });
    expect(seen).toEqual([4]);
    unsubscribe();
  });
});

describe('contextBudget (§2.6)', () => {
  it('caps everything at the 265k ceiling but keeps smaller native windows', () => {
    const { deps } = makeDeps();
    expect(contextBudget(deps, 'gpt-4o')).toBe(128_000);
    expect(contextBudget(deps, 'claude-3-5-sonnet')).toBe(200_000);
    expect(contextBudget(deps, 'gemini-1.5-pro')).toBe(265_000);
    expect(contextBudget(deps, 'unknown-model')).toBe(128_000); // resolveModel fallback
  });
});
