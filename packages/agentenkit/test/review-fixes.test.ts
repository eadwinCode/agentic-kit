import { UnknownAgentError } from '../src/runtime.js';
import { DEL_IF_VALUE_SCRIPT, SET_IF_VALUE_SCRIPT } from '../src/adapters/upstash.js';
import { describe, expect, it } from 'bun:test';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { bindStorage } from '../src/core/state.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { RedisKv } from '../src/adapters/redis.js';
import type { RuntimeOptions } from '../src/ports/runtime.js';
import type { AgentConfig } from '../src/core/types.js';
import type { AgentConfig as AgentConfigType } from '../src/core/types.js';
import { resolveConfig } from '../src/core/types.js';

async function makeDeps(config: Partial<AgentConfig> = {}) {
  const storage = new MemoryStorage();
  const bus = new MemoryBus();
  const queue = new MemoryQueue();
  const kv = new MemoryKv();
  const deps: RuntimeOptions = {
    storage,
    // Isolated per test: the default store is a file on disk.
    admin: new MemoryAdminStore(),
    bus,
    queue,
    kv,
    resolveModel: (name) => ({
      instance: () => {
        throw new Error('no-llm');
      },
      contextWindow: 128_000,
    }),
    config: resolveConfig(config),
  };
  return { deps, store: bindStorage(storage, { state: {} }), runtime: await setupAgentCore(deps), storage, bus, queue, kv };
}

function chatHandle(runtime: Awaited<ReturnType<typeof setupAgentCore>>) {
  return runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
}

describe('per-thread run lock (§2.8, §3.4)', () => {
  it('a second worker is a no-op while the lock is held', async () => {
    const { deps, store, runtime, queue } = await makeDeps();
    const chat = chatHandle(runtime);
    const ran = await chat.run({ prompt: 'a' });

    // Simulate a live worker holding the lock (engine acquires it in execute)
    const locked = await deps.kv.set(`agent:lock:${ran.threadId}`, 'worker-1', {
      onlyIfNotExists: true,
      exSeconds: deps.config!.runLockLeaseSeconds,
    });
    expect(locked).toBe(true);

    // A second dispatch (at-least-once redelivery) must not double-execute:
    // the queue already holds run()'s original job — it must not grow
    const before = queue.items.length;
    await chat.executeWithPolicy({ threadId: ran.threadId, model: 'gpt-4o' });
    expect(queue.items.length).toBe(before);
  });

  it('a lock-conflict no-op preserves the retry counter', async () => {
    const { deps, store, runtime } = await makeDeps();
    const chat = chatHandle(runtime);
    const ran = await chat.run({ prompt: 'a' });

    // A past failure left an attempt behind, and another worker holds the lock
    await deps.kv.set(`agent:attempts:${ran.threadId}`, '1');
    await deps.kv.set(`agent:lock:${ran.threadId}`, 'other-worker', {
      onlyIfNotExists: true,
      exSeconds: deps.config!.runLockLeaseSeconds,
    });

    await chat.executeWithPolicy({ threadId: ran.threadId, model: 'gpt-4o' });
    // The no-op must not reset the budget while the owning worker still runs
    expect(await deps.kv.get(`agent:attempts:${ran.threadId}`)).toBe('1');
  });

  it('Kv.set with onlyIfNotExists is SET-NX semantics across adapters', async () => {
    const kv = new MemoryKv();
    expect(await kv.set('k', 'v1', { onlyIfNotExists: true })).toBe(true);
    expect(await kv.set('k', 'v2', { onlyIfNotExists: true })).toBe(false);
    expect(await kv.get('k')).toBe('v1');

    // Redis adapter: 'OK' on success, null on NX conflict (node-redis)
    const store = new Map<string, string>();
    const fakeRedis = {
      async connect() {},
      async get(k: string) { return store.get(k) ?? null; },
      async set(k: string, v: string, opts?: { EX?: number; NX?: boolean }) {
        if (opts?.NX && store.has(k)) return null;
        store.set(k, v);
        return 'OK';
      },
      async del(...k: string[]) { k.forEach((x) => store.delete(x)); },
      async incr(k: string) {
        const n = Number(store.get(k) ?? 0) + 1;
        store.set(k, String(n));
        return n;
      },
      async publish() {},
      // Acts out the two compare-and-act scripts, in node-redis's call shape.
      async eval(script: string, a: { keys: string[]; arguments: string[] }) {
        const [key] = a.keys;
        const [expected, value] = a.arguments;
        if (store.get(key) !== expected) return 0;
        if (script === SET_IF_VALUE_SCRIPT) store.set(key, value);
        else if (script === DEL_IF_VALUE_SCRIPT) store.delete(key);
        return 1;
      },
      duplicate(): never { throw new Error('not used'); },
    };
    const redisKv = new RedisKv(fakeRedis);
    expect(await redisKv.set('k', 'a', { onlyIfNotExists: true })).toBe(true);
    expect(await redisKv.set('k', 'b', { onlyIfNotExists: true })).toBe(false);
    expect(await redisKv.get('k')).toBe('a');
    // Compare-and-act: only the holder of the value may renew or free it
    expect(await redisKv.setIfValue('k', 'b', 'b', { exSeconds: 30 })).toBe(false);
    expect(await redisKv.setIfValue('k', 'a', 'a', { exSeconds: 30 })).toBe(true);
    expect(await redisKv.delIfValue('k', 'b')).toBe(false);
    expect(await redisKv.get('k')).toBe('a');
    expect(await redisKv.delIfValue('k', 'a')).toBe(true);
    expect(await redisKv.get('k')).toBe(null);
  });
});

describe('executeWithPolicy failure handling (§2.8)', () => {
  it('redrives transient failures, then finalizes FAILED on BOTH cache and durable state', async () => {
    const { deps, store, runtime, queue } = await makeDeps({
      // resolveModel hands out instances that always throw → deterministic failure
      runMaxAttempts: 2,
    });
    const chat = chatHandle(runtime);
    const ran = await chat.run({ prompt: 'a' });
    const initialJobs = queue.items.length; // run() enqueues the first job itself

    await chat.executeWithPolicy({ threadId: ran.threadId, model: 'gpt-4o' });
    expect(queue.items.length).toBe(initialJobs + 1); // redriven once

    await chat.executeWithPolicy({ threadId: ran.threadId, model: 'gpt-4o' });
    // Attempts exhausted → FAILED on both homes (the review's critical #3)
    expect(await deps.kv.get(`agent:state:${ran.threadId}`)).toBe('FAILED');
    const thread = await store.threads.get(ran.threadId);
    expect(thread!.state).toBe('FAILED');
    expect(queue.items.length).toBe(initialJobs + 1); // no further redrive
  });

  it('resets the attempt counter after a successful run', async () => {
    const { deps, store, runtime } = await makeDeps();
    const chat = chatHandle(runtime);
    const ran = await chat.run({ prompt: 'a' });

    // Simulate a past failure leaving a stale counter behind
    await deps.kv.set(`agent:attempts:${ran.threadId}`, '2');

    // Force the success path by stubbing the engine execute through the
    // runtime's worker: success must clear the counter (§2.8)
    const rt = runtime as unknown as {
      worker: { handleJob: (job: any) => Promise<unknown> };
    };
    void rt;
    void deps;
    // Direct engine-level contract check instead: success → counter cleared
    const { executeWithPolicy: policyFn } = await import('../src/core/engine.js');
    const fakeAgent = {
      name: 'chat',
      kind: 'stream-text' as const,
      spec: {},
      args: {},
      sem: { acquire: async () => () => {} },
    };
    await policyFn(
      { ...deps, storage: store } as any,
      fakeAgent as any,
      { threadId: ran.threadId, model: 'gpt-4o' },
      undefined,
      // injected execute — simulates success without touching an LLM
      (async () => 'executed') as any,
    );
    expect(await deps.kv.get(`agent:attempts:${ran.threadId}`)).toBeNull();
  });
});

describe('MemoryKv.incr atomicity (§3.4)', () => {
  it('concurrent incr calls produce unique monotonic seqs', async () => {
    const kv = new MemoryKv();
    const results = await Promise.all(
      Array.from({ length: 100 }, () => kv.incr('agent:seq:t1')),
    );
    expect(new Set(results).size).toBe(100); // no collisions
    expect(await kv.get('agent:seq:t1')).toBe('100');
  });
});

describe('runtime.worker.handleJob (§2.8)', () => {
  // A job for an agent this process does not have is an error the queue
  // keeps and retries, never a silent success that deletes the job. The same
  // case runs in the Go package.
  it('an unknown agent is an error, not a silent success', async () => {
    const { runtime } = await makeDeps();
    await expect(
      runtime.worker.handleJob({ threadId: 't1', model: 'gpt-4o', agent: 'nope' }),
    ).rejects.toBeInstanceOf(UnknownAgentError);
  });

  it('dispatches to the default handle when agent is omitted', async () => {
    const { deps, store, runtime, queue } = await makeDeps();
    runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const thread = await store.threads.create(undefined);

    const res = await runtime.worker.handleJob({ threadId: thread.id, model: 'gpt-4o' });
    expect(res.accepted).toBe(true);
    // execute threw (no-llm) → §2.8 redrive enqueued one job
    expect(queue.items.length).toBe(1);
    expect(queue.items[0]!.agent).toBe('chat');
  });

  it('a job for a missing (deleted) thread is a no-op', async () => {
    const { runtime, queue } = await makeDeps();
    runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });

    const res = await runtime.worker.handleJob({ threadId: 'deleted-thread', model: 'gpt-4o' });
    expect(res.accepted).toBe(true);
    expect(queue.items.length).toBe(0); // nothing executed, nothing redriven
  });
});
