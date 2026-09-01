import { describe, expect, it } from 'bun:test';
import { setupAgentCore } from '../src/runtime.js';
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
  return { deps, runtime: setupAgentCore(deps), storage, bus, queue, kv };
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

  it('re-dispatches a true orphan instead of healing it inline (§2.7)', async () => {
    const { deps, runtime, queue } = makeDeps({ hitlTtlMs: 5, reclaimGraceMs: 1 });
    const thread = await deps.storage.threads.create();
    const seq = await deps.kv.incr(`agent:seq:${thread.id}`);
    await deps.storage.threads.setState(thread.id, 'WAITING_FOR_INPUT');
    await deps.kv.set(`agent:state:${thread.id}`, 'WAITING_FOR_INPUT');
    await deps.kv.set(`agent:run:${thread.id}`, 'run-1');
    await deps.storage.events.append(thread.id, {
      threadId: thread.id, seq, type: 'INPUT_REQUIRED', payload: { toolCallId: 'c1' },
      createdAt: new Date(Date.now() - 60_000), // far older than TTL + grace
    });

    expect(await runtime.hitl.reclaimIfOrphaned(thread.id)).toBe(true);

    // It kicks the engine and touches nothing else: the engine owns the single
    // definition of what an expired approval becomes, and resolves a thread's
    // open approvals as a set rather than one at a time (§2.7).
    expect(await deps.storage.messages.list(thread.id)).toHaveLength(0);
    expect((await deps.storage.threads.get(thread.id))!.state).toBe('WAITING_FOR_INPUT');
    expect(queue.items).toHaveLength(1);
    // Resuming reuses the parked run's id (§2.1)
    expect(queue.items[0]).toMatchObject({ threadId: thread.id, runId: 'run-1' });

    // A duplicate re-dispatch is safe — the run lock and the engine's
    // readiness check make it a no-op, so it is not suppressed here.
    expect(await runtime.hitl.reclaimIfOrphaned(thread.id)).toBe(true);
    expect(queue.items).toHaveLength(2);
  });

  it('waits while ANY open approval is still answerable (§2.7)', async () => {
    const { deps, runtime, queue } = makeDeps({ hitlTtlMs: 5, reclaimGraceMs: 1 });
    const thread = await deps.storage.threads.create();
    await deps.storage.threads.setState(thread.id, 'WAITING_FOR_INPUT');
    for (const [id, ageMs] of [['old', 60_000], ['fresh', 0]] as const) {
      await deps.storage.events.append(thread.id, {
        threadId: thread.id,
        seq: await deps.kv.incr(`agent:seq:${thread.id}`),
        type: 'INPUT_REQUIRED',
        payload: { toolCallId: id },
        createdAt: new Date(Date.now() - ageMs),
      });
    }

    expect(await runtime.hitl.reclaimIfOrphaned(thread.id)).toBe(false);
    expect(queue.items).toHaveLength(0);
  });
});

describe('runtime.deleteThread (§3.2)', () => {
  it('deletes the thread and everything that follows it', async () => {
    const { deps, storage, runtime, bus, kv } = makeDeps();
    const thread = await deps.storage.threads.create();
    await deps.storage.messages.append(thread.id, { role: 'user', content: 'hi' });
    await deps.storage.events.append(thread.id, {
      threadId: thread.id, seq: 1, type: 'CHUNK', payload: {}, createdAt: new Date(),
    });
    await deps.storage.usage.record(thread.id, {
      agentId: null, inputTokens: 1, cachedInputTokens: 0, outputTokens: 1, totalTokens: 2,
    });
    await deps.storage.runs.start({ id: 'sub-1', threadId: thread.id, agent: 'sub', model: 'gpt-4o', depth: 1 });
    await deps.kv.set(`agent:state:${thread.id}`, 'COMPLETED');
    await deps.kv.set(`agent:seq:${thread.id}`, '1');
    await deps.kv.set(`agent:attempts:${thread.id}`, '2');

    const res = await runtime.deleteThread(thread.id);
    expect(res.accepted).toBe(true);

    // Cascade: messages, events, usage, runs all follow the thread
    expect(await deps.storage.threads.get(thread.id)).toBeNull();
    expect(await deps.storage.messages.list(thread.id)).toEqual([]);
    expect(await deps.storage.events.listSince(thread.id, -1)).toEqual([]);
    expect(storage.usage.recorded.filter((u) => u.threadId === thread.id)).toEqual([]);
    expect([...storage.runs.store.values()]).toEqual([]);

    // Hot cache cleanup — no resurrection from kv
    for (const key of ['state', 'seq', 'attempts', 'lock']) {
      expect(await kv.get(`agent:${key}:${thread.id}`)).toBeNull();
    }

    // Live UIs are told via the bus-only notice
    expect(bus.published.at(-1)!.type).toBe('THREAD_DELETED');
  });

  it('refuses an active run but deletes a parked HITL thread (no process held, §2.5)', async () => {
    const { deps, runtime } = makeDeps();
    const running = await deps.storage.threads.create();
    await deps.storage.threads.setState(running.id, 'RUNNING');
    const refused = await runtime.deleteThread(running.id);
    expect(refused.accepted).toBe(false);
    expect(refused.error).toMatch(/stop/i);
    expect(await deps.storage.threads.get(running.id)).not.toBeNull();

    const parked = await deps.storage.threads.create();
    await deps.storage.threads.setState(parked.id, 'WAITING_FOR_INPUT');
    expect((await runtime.deleteThread(parked.id)).accepted).toBe(true);
  });

  it('rejects an unknown thread', async () => {
    const { runtime } = makeDeps();
    const res = await runtime.deleteThread('nope');
    expect(res.accepted).toBe(false);
    expect(res.error).toMatch(/not found/i);
  });

  it('a late resume dispatch after deletion is a no-op — no resurrection', async () => {
    const { deps, runtime } = makeDeps();
    const thread = await deps.storage.threads.create();
    await deps.storage.threads.setState(thread.id, 'WAITING_FOR_INPUT');
    await deps.storage.events.append(thread.id, {
      threadId: thread.id, seq: 1, type: 'INPUT_REQUIRED',
      payload: { toolCallId: 'c1', toolName: 'sendEmail', resume: { agent: 'chat', model: 'gpt-4o' } },
      createdAt: new Date(),
    });

    expect((await runtime.deleteThread(thread.id)).accepted).toBe(true);
    // The §2.5 resume job arrives after the delete — nothing may come back
    await runtime.worker.handleJob({ threadId: thread.id, model: 'gpt-4o', agent: 'chat' });
    expect(await deps.storage.threads.get(thread.id)).toBeNull();
    expect(await deps.storage.messages.list(thread.id)).toEqual([]);
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
