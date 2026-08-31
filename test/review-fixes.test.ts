import { describe, expect, it } from 'bun:test';
import { createAgentRuntime } from '../src/runtime.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import type { AgentConfig } from '../src/core/types.js';
import { resolveConfig } from '../src/core/types.js';
import type { RuntimePorts } from '../src/ports/runtime.js';
import { RedisKv } from '../src/adapters/redis.js';

function makeDeps(config: Partial<AgentConfig> = {}) {
  const storage = new MemoryStorage();
  const bus = new MemoryBus();
  const queue = new MemoryQueue();
  const kv = new MemoryKv();
  const deps: RuntimePorts = { storage, bus, queue, kv, models: {}, config: resolveConfig(config) };
  return { deps, runtime: createAgentRuntime(deps), storage, bus, queue, kv };
}

describe('per-thread run lock (§2.8, §3.4)', () => {
  it('a second worker is a no-op while the lock is held', async () => {
    const { deps, runtime, queue } = makeDeps();
    const ran = await runtime.run({ prompt: 'a', model: 'gpt-4o' });

    // Simulate a live worker holding the lock (engine acquires it in execute)
    const locked = await deps.kv.set(`agent:lock:${ran.threadId}`, 'worker-1', {
      onlyIfNotExists: true,
      exSeconds: deps.config.runLockLeaseSeconds,
    });
    expect(locked).toBe(true);

    // A second dispatch (at-least-once redelivery) must not double-execute:
    // the queue already holds run()'s original job — it must not grow
    const before = queue.items.length;
    await runtime.engine.executeWithPolicy({ threadId: ran.threadId, model: 'gpt-4o' });
    expect(queue.items.length).toBe(before);
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
      duplicate(): never { throw new Error('not used'); },
    };
    const redisKv = new RedisKv(fakeRedis);
    expect(await redisKv.set('k', 'a', { onlyIfNotExists: true })).toBe(true);
    expect(await redisKv.set('k', 'b', { onlyIfNotExists: true })).toBe(false);
    expect(await redisKv.get('k')).toBe('a');
  });
});

describe('executeWithPolicy failure handling (§2.8)', () => {
  it('redrives transient failures, then finalizes FAILED on BOTH cache and durable state', async () => {
    const { deps, runtime, queue } = makeDeps({
      // models registry is empty → execute always throws (deterministic failure)
      runMaxAttempts: 2,
    });
    const ran = await runtime.run({ prompt: 'a', model: 'gpt-4o' });
    const initialJobs = queue.items.length; // run() enqueues the first job itself

    await runtime.engine.executeWithPolicy({ threadId: ran.threadId, model: 'gpt-4o' });
    expect(queue.items.length).toBe(initialJobs + 1); // redriven once

    await runtime.engine.executeWithPolicy({ threadId: ran.threadId, model: 'gpt-4o' });
    // Attempts exhausted → FAILED on both homes (the review's critical #3)
    expect(await deps.kv.get(`agent:state:${ran.threadId}`)).toBe('FAILED');
    const thread = await deps.storage.threads.get(ran.threadId);
    expect(thread!.state).toBe('FAILED');
    expect(queue.items.length).toBe(initialJobs + 1); // no further redrive
  });

  it('resets the attempt counter after a successful run', async () => {
    const { deps, runtime } = makeDeps();
    const ran = await runtime.run({ prompt: 'a', model: 'gpt-4o' });

    // Simulate a past failure leaving a stale counter behind
    await deps.kv.set(`agent:attempts:${ran.threadId}`, '2');

    // Engine fails fast here (no models), but the counter must never persist
    // across redrives… force the SUCCESS path by asserting the reset happens
    // on success via a stubbed execute:
    const deps2 = { ...deps, models: {} as Record<string, unknown> };
    void deps2;
    // Direct check of the policy contract: successful execute → counter cleared
    const spyExecute = async () => { /* success */ };
    void spyExecute;
    const policy = await (async () => {
      // call executeWithPolicy but stub engine.execute through runtime override
      const rt = runtime as unknown as { engine: { execute: (i: unknown) => Promise<void> } };
      rt.engine.execute = async () => undefined; // simulate success
      await runtime.engine.executeWithPolicy({ threadId: ran.threadId, model: 'gpt-4o' });
      return deps.kv.get(`agent:attempts:${ran.threadId}`);
    })();
    expect(policy).toBeNull();
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
