import { describe, expect, it } from 'bun:test';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import type { AgentConfig } from '../src/core/types.js';

async function make(config: Partial<AgentConfig> = {}) {
  const storage = new MemoryStorage(), kv = new MemoryKv(), queue = new MemoryQueue(), admin = new MemoryAdminStore();
  const runtime = await setupAgentCore({
    storage, kv, queue, admin, bus: new MemoryBus(), config,
    resolveModel: () => { throw new Error('worker must not execute a rejected dispatch'); },
  });
  return { storage, kv, queue, admin, runtime, chat: runtime.createStreamTextAgent({ name: 'chat' }) };
}

describe('run admission and dispatch recovery', () => {
  for (const cached of [false, true]) it(`accepts only one concurrent send (${cached ? 'cached completed state' : 'missing cache'})`, async () => {
    let arrivals = 0;
    let release!: () => void;
    const gate = new Promise<void>((r) => { release = r; });
    const r = await make({ billingPreCheck: async () => {
      if (++arrivals === 2) release();
      await gate;
      return { ok: true };
    } });
    const thread = await r.storage.threads.create({ model: 'gpt-4o' });
    if (cached) {
      await r.storage.threads.setState(thread.id, 'COMPLETED');
      await r.kv.set(`agent:state:${thread.id}`, 'COMPLETED');
    }
    const results = await Promise.all(['a', 'b'].map((prompt) => r.chat.run({ threadId: thread.id, prompt })));
    expect(results.filter((x) => x.accepted)).toHaveLength(1);
    expect(results.find((x) => !x.accepted)!.error).toContain('active run');
    expect(await r.storage.messages.list(thread.id)).toHaveLength(1);
    expect(await r.admin.runs.listByThread(thread.id)).toHaveLength(1);
    expect(r.queue.items).toHaveLength(1);
  });

  it('refuses a durable active run even if its hot state was lost', async () => {
    const r = await make();
    const first = await r.chat.run({ prompt: 'first' });
    await r.kv.del(`agent:state:${first.threadId}`);
    expect((await r.chat.run({ threadId: first.threadId, prompt: 'second' })).accepted).toBe(false);
    expect(r.queue.items).toHaveLength(1);
  });

  it('closes a rejected dispatch and accepts a later send; ambiguous redelivery is a no-op', async () => {
    const r = await make();
    const enqueue = r.queue.enqueue.bind(r.queue);
    r.queue.enqueue = async (job) => { await enqueue(job); throw new Error('queue unavailable'); };
    const failed = await r.chat.run({ prompt: 'first' });
    expect(failed).toMatchObject({ accepted: false, error: 'queue unavailable' });
    expect((await r.storage.threads.get(failed.threadId))!.state).toBe('FAILED');
    expect(await r.kv.get(`agent:state:${failed.threadId}`)).toBe('FAILED');
    const record = (await r.admin.runs.get(failed.runId!))!;
    expect(record).toMatchObject({ state: 'FAILED', stopReason: 'failed', error: 'queue unavailable' });
    expect(record.endedAt).toBeInstanceOf(Date);
    expect(record.durationMs).toBeGreaterThanOrEqual(0);
    await r.runtime.worker.handleJob(r.queue.items[0]!);
    expect((await r.admin.runs.get(failed.runId!))!.state).toBe('FAILED');
    r.queue.enqueue = enqueue;
    expect((await r.chat.run({ threadId: failed.threadId, prompt: 'retry' })).accepted).toBe(true);
  });

  it('releases admission when persistence fails before enqueue', async () => {
    const r = await make();
    const append = r.storage.messages.append.bind(r.storage.messages);
    r.storage.messages.append = async () => { throw new Error('storage unavailable'); };
    const failed = await r.chat.run({ prompt: 'first' });
    expect(failed.accepted).toBe(false);
    expect(await r.kv.get(`agent:state:${failed.threadId}`)).toBe('FAILED');
    r.storage.messages.append = append;
    expect((await r.chat.run({ threadId: failed.threadId, prompt: 'retry' })).accepted).toBe(true);
  });

  it('releases the durable claim if installing the run identity fails', async () => {
    const r = await make();
    const set = r.kv.set.bind(r.kv);
    let fail = true;
    r.kv.set = async (key, value, opts) => {
      if (fail && key.startsWith('agent:run:')) { fail = false; throw new Error('temporary cache failure'); }
      return set(key, value, opts);
    };
    const failed = await r.chat.run({ prompt: 'first' });
    expect(failed.accepted).toBe(false);
    expect((await r.storage.threads.get(failed.threadId))!.state).toBe('FAILED');
    expect((await r.chat.run({ threadId: failed.threadId, prompt: 'retry' })).accepted).toBe(true);
  });

  it('does not let a late enqueue failure overwrite a stop and newer run', async () => {
    const r = await make();
    let entered!: () => void, reject!: (err: Error) => void;
    const ready = new Promise<void>((resolve) => { entered = resolve; });
    const enqueue = r.queue.enqueue.bind(r.queue);
    r.queue.enqueue = async () => { entered(); await new Promise<void>((_, no) => { reject = no; }); };
    const first = r.chat.run({ prompt: 'first' });
    await ready;
    const thread = (await r.storage.threads.list())[0]!;
    await r.chat.stop(thread.id);
    r.queue.enqueue = enqueue;
    const second = await r.chat.run({ threadId: thread.id, prompt: 'second' });
    reject(new Error('late failure'));
    const failed = await first;
    expect((await r.admin.runs.get(failed.runId!))!.state).toBe('CANCELLED');
    expect((await r.admin.runs.get(second.runId!))!.state).toBe('RUNNING');
    expect((await r.storage.threads.get(thread.id))!.state).toBe('RUNNING');
  });

  it('honours a stop while the admitted send is still persisting its message', async () => {
    const r = await make();
    const append = r.storage.messages.append.bind(r.storage.messages);
    let entered!: () => void, release!: () => void;
    const ready = new Promise<void>((resolve) => { entered = resolve; });
    const gate = new Promise<void>((resolve) => { release = resolve; });
    r.storage.messages.append = async (...args) => { entered(); await gate; return append(...args); };
    const pending = r.chat.run({ prompt: 'first' });
    await ready;
    const thread = (await r.storage.threads.list())[0]!;
    expect((await r.chat.stop(thread.id)).accepted).toBe(true);
    release();
    const stopped = await pending;
    expect(stopped.accepted).toBe(false);
    expect((await r.admin.runs.get(stopped.runId!))!.state).toBe('CANCELLED');
    expect((await r.storage.threads.get(thread.id))!.state).toBe('CANCELLED');
    expect(await r.kv.get(`agent:state:${thread.id}`)).toBe('CANCELLED');
    expect(r.queue.items).toHaveLength(0);
  });
});
