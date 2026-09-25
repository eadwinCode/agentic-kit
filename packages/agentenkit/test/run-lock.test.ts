import { describe, expect, it } from 'bun:test';
import { simulateReadableStream } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { Lease, parseLockValue, runLockKey } from '../src/core/lease.js';
import { resolveConfig, type AgentConfig } from '../src/core/types.js';
import type { RuntimePorts } from '../src/ports/runtime.js';

// Workstream B: the run lock names its holder's run, delivery and grip, is
// renewed while held, and is only ever freed by the holder that took it.

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** A model whose call takes `latencyMs`, and fails when the run aborts. */
function slowModel(latencyMs = 50) {
  return new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock-slow',
    doStream: async ({ abortSignal }: any) => {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(resolve, latencyMs);
        abortSignal?.addEventListener('abort', () => {
          clearTimeout(timer);
          reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
        });
      });
      const chunks: LanguageModelV1StreamPart[] = [
        { type: 'text-delta', textDelta: 'reply' },
        { type: 'finish', finishReason: 'stop', usage: { promptTokens: 10, completionTokens: 5 } },
      ];
      return { stream: simulateReadableStream({ chunks }), rawCall: { rawPrompt: null, rawSettings: {} } };
    },
  });
}

async function makeRuntime(model: any, config: Partial<AgentConfig> = {}) {
  const storage = new MemoryStorage();
  const queue = new MemoryQueue();
  const kv = new MemoryKv();
  const runtime = await setupAgentCore({
    storage, bus: new MemoryBus(), queue, kv,
    admin: new MemoryAdminStore(),
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
    config: resolveConfig({ stopPollMs: 20, ...config }),
  });
  const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
  return { runtime, chat, storage, queue, kv };
}

const leaseDeps = (runLockLeaseSeconds: number) =>
  ({
    kv: new MemoryKv(),
    config: resolveConfig({ runLockLeaseSeconds }),
    log: { error: () => undefined },
  }) as unknown as RuntimePorts;

describe('Lease', () => {
  it('parses a lock value, and a bare run id from before dispatch ids', () => {
    expect(parseLockValue('r1/d1/n1')).toEqual({ runId: 'r1', dispatchId: 'd1' });
    expect(parseLockValue('r1')).toEqual({ runId: 'r1', dispatchId: null });
    expect(parseLockValue(null)).toEqual({ runId: null, dispatchId: null });
  });

  it('one holder at a time, even for the same job', async () => {
    const deps = leaseDeps(60);
    const a = await Lease.acquire(deps, 't1', 'r1', 'd1');
    expect(a).not.toBeNull();
    expect(await Lease.acquire(deps, 't1', 'r1', 'd1')).toBeNull();
    await a!.release();
    const c = await Lease.acquire(deps, 't1', 'r1', 'd1');
    expect(c).not.toBeNull();
    await c!.release();
  });

  // The case the nonce exists for: a stalled worker's lease lapses and the
  // queue's redelivery of the SAME job takes the lock.
  it('a stalled holder learns it lost the lock, and cannot free the new holder\'s', async () => {
    const deps = leaseDeps(1);
    const stalled = (await Lease.acquire(deps, 't1', 'r1', 'd1'))!;
    await deps.kv.del(runLockKey('t1')); // its lease lapsed while it stalled
    const fresh = (await Lease.acquire(deps, 't1', 'r1', 'd1'))!;
    expect(fresh).not.toBeNull();

    let lost = false;
    stalled.keep(() => (lost = true));
    for (let i = 0; i < 40 && !lost; i++) await sleep(25);
    expect(lost).toBe(true);
    expect(stalled.lost).toBe(true);

    await stalled.release();
    expect(parseLockValue(await deps.kv.get(runLockKey('t1'))).runId).toBe('r1'); // still the new holder's
    await fresh.release();
    expect(await deps.kv.get(runLockKey('t1'))).toBeNull();
  });

  it('a kept lease outlives its own length', async () => {
    const deps = leaseDeps(1);
    const lease = (await Lease.acquire(deps, 't1', 'r1', undefined))!;
    lease.keep();
    await sleep(1_400);
    expect(await deps.kv.get(runLockKey('t1'))).not.toBeNull();
    expect(lease.lost).toBe(false);
    await lease.release();
  });
});

describe('execute and the run lock', () => {
  it('keeps the lock through a step longer than the lease, then frees it', async () => {
    const { chat, queue, kv } = await makeRuntime(slowModel(1_400), { runLockLeaseSeconds: 1 });
    const ran = await chat.run({ prompt: 'a' });
    const job = queue.items[0]!;
    const running = chat.executeWithPolicy({ ...job, model: 'gpt-4o' });
    await sleep(1_200); // past the lease: only a renewal keeps it
    expect(parseLockValue(await kv.get(runLockKey(ran.threadId))).runId).toBe(ran.runId!);
    await running;
    expect(await kv.get(runLockKey(ran.threadId))).toBeNull();
    expect(await kv.get(`agent:state:${ran.threadId}`)).toBe('COMPLETED');
  }, 10_000);

  it('a lost lock ends the segment without finalizing it as a stop, and the job comes back', async () => {
    const { chat, queue, kv, storage } = await makeRuntime(slowModel(1_500), { runLockLeaseSeconds: 1 });
    const ran = await chat.run({ prompt: 'a' });
    const job = queue.items[0]!;
    const running = chat.executeWithPolicy({ ...job, model: 'gpt-4o' });
    await sleep(50);
    // Another holder takes over (a lapsed lease the queue redelivered).
    await kv.set(runLockKey(ran.threadId), `${ran.runId}/another-delivery/w2`);
    await running;
    expect(await kv.get(`agent:state:${ran.threadId}`)).toBe('RUNNING'); // not CANCELLED
    expect((await storage.threads.get(ran.threadId))!.state).toBe('RUNNING');
    expect(await kv.get(runLockKey(ran.threadId))).toBe(`${ran.runId}/another-delivery/w2`); // left alone
    expect(queue.items).toHaveLength(2); // it comes back once the lock is free
    expect(queue.items[1]!.runId).toBe(ran.runId);
  }, 10_000);
});

describe('lock conflicts', () => {
  it('drops the queue\'s duplicate of the job that holds the lock', async () => {
    const { chat, queue, kv } = await makeRuntime(slowModel());
    const ran = await chat.run({ prompt: 'a' });
    const job = queue.items[0]!;
    expect(job.dispatchId).toBeTruthy(); // every enqueue is stamped
    await kv.set(runLockKey(ran.threadId), `${ran.runId}/${job.dispatchId}/other-worker`);
    await chat.executeWithPolicy({ ...job, model: 'gpt-4o' });
    expect(queue.items).toHaveLength(1);
  });

  it('redrives another delivery of the running run instead of dropping it', async () => {
    const { chat, queue, kv } = await makeRuntime(slowModel());
    const ran = await chat.run({ prompt: 'a' });
    const job = queue.items[0]!;
    // Say the failed segment that queued this retry has not let go yet.
    // Dropping the retry would leave the thread RUNNING with nobody on it.
    await kv.set(runLockKey(ran.threadId), `${ran.runId}/earlier-delivery/w1`);
    await chat.executeWithPolicy({ ...job, model: 'gpt-4o' });
    expect(queue.items).toHaveLength(2);
    expect(queue.items[1]!.dispatchId).not.toBe(job.dispatchId); // a delivery of its own
  });

  // The fast-answer case: an approval answered while the parking segment
  // still holds the lock used to be dropped as a duplicate, and then expired.
  it('redrives a resume that arrives while the parking segment still holds the lock', async () => {
    const { chat, queue, kv, storage } = await makeRuntime(slowModel());
    const ran = await chat.run({ prompt: 'a' });
    const job = queue.items[0]!;
    const current = (await storage.threads.get(ran.threadId))!.state;
    await storage.threads.claimState(ran.threadId, current, 'WAITING_FOR_INPUT');
    await kv.set(runLockKey(ran.threadId), ran.runId!); // a lock from before dispatch ids
    await chat.executeWithPolicy({ ...job, model: 'gpt-4o' });
    expect(queue.items).toHaveLength(2);
  });

  it('waits longer each time, up to the lease, before giving up', async () => {
    const { chat, queue, kv } = await makeRuntime(slowModel(), {
      runRedriveDelaySeconds: 1,
      runLockLeaseSeconds: 5,
      runMaxAttempts: 1,
    });
    const ran = await chat.run({ prompt: 'a' });
    await kv.set(runLockKey(ran.threadId), 'an-older-run/d/n');
    const job = { threadId: ran.threadId, runId: ran.runId, model: 'gpt-4o' };
    for (let i = 0; i < 5; i++) await chat.executeWithPolicy(job);
    // 1s, 2s, 4s; then, having waited 7s (over one lease) with its attempts
    // spent, the fourth arrival gives up.
    expect(queue.delays.slice(1)).toEqual([1, 2, 4]);
    expect(await kv.get(`agent:state:${ran.threadId}`)).toBe('FAILED');
  });
});
