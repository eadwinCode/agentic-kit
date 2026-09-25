import { describe, expect, it } from 'bun:test';
import { Database } from 'bun:sqlite';
import { simulateReadableStream } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { SqliteStorage, type SqliteLike } from '../src/adapters/sqlite.js';
import { bindStorage } from '../src/core/state.js';
import { finalize } from '../src/core/engine.js';
import { parkForApproval } from '../src/core/hitl.js';
import { transition } from '../src/core/publish.js';
import { attemptsKey, runIdKey } from '../src/core/keys.js';
import { resolveConfig, type AgentConfig, type ExecutionState } from '../src/core/types.js';
import type { Storage } from '../src/ports/storage.js';
import type { RuntimePorts } from '../src/ports/runtime.js';

// Workstream C: every run state change is one compare-and-set on the state
// AND the run that owns the thread. The same cases run in the Go package
// (state_transition_test.go).

/** What every Storage adapter's `transition` must do. */
async function transitionContract(s: Storage) {
  const ctx = { state: {} };
  const th = await s.threads.create({}, ctx);
  const step = async (
    name: string,
    t: { from: ExecutionState[]; to: ExecutionState; runId?: string; newRunId?: string },
    won: boolean,
    state: ExecutionState,
  ) => {
    expect([name, await s.threads.transition(th.id, t, ctx)]).toEqual([name, won]);
    expect([name, (await s.threads.get(th.id, ctx))!.state]).toEqual([name, state]);
  };
  await step('admission records the run', { from: ['IDLE'], to: 'QUEUED', newRunId: 'r1' }, true, 'QUEUED');
  await step('wrong state loses', { from: ['RUNNING'], to: 'COMPLETED', runId: 'r1' }, false, 'QUEUED');
  await step('another run loses', { from: ['QUEUED'], to: 'RUNNING', runId: 'r2' }, false, 'QUEUED');
  await step('its own run wins', { from: ['QUEUED'], to: 'RUNNING', runId: 'r1' }, true, 'RUNNING');
  await step('any of several states', { from: ['QUEUED', 'RUNNING', 'WAITING_FOR_INPUT'], to: 'CANCELLED', runId: 'r1' }, true, 'CANCELLED');
  await step('a new run is admitted', { from: ['CANCELLED'], to: 'QUEUED', newRunId: 'r2' }, true, 'QUEUED');
  await step('the replaced run can no longer move it', { from: ['QUEUED'], to: 'CANCELLED', runId: 'r1' }, false, 'QUEUED');
  await step('no run named matches any run', { from: ['QUEUED'], to: 'RUNNING' }, true, 'RUNNING');

  // A thread from before the run was recorded matches any run.
  const legacy = await s.threads.create({}, ctx);
  expect(await s.threads.transition(legacy.id, { from: ['IDLE'], to: 'FAILED', runId: 'x' }, ctx)).toBe(true);
  expect(await s.threads.transition('no-such-thread', { from: ['IDLE'], to: 'QUEUED' }, ctx)).toBe(false);
}

describe('Storage.threads.transition', () => {
  it('MemoryStorage', () => transitionContract(new MemoryStorage()));
  it('SqliteStorage', () => transitionContract(new SqliteStorage(new Database(':memory:') as unknown as SqliteLike)));
});

function model(steps: Array<'fail' | 'ok'>, before?: () => Promise<void>) {
  let call = 0;
  return new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock',
    doStream: async () => {
      const s = steps[Math.min(call++, steps.length - 1)];
      await before?.();
      if (s === 'fail') throw new Error('boom');
      const chunks: LanguageModelV1StreamPart[] = [
        { type: 'text-delta', textDelta: 'ok' },
        { type: 'finish', finishReason: 'stop', usage: { promptTokens: 10, completionTokens: 5 } },
      ];
      return { stream: simulateReadableStream({ chunks }), rawCall: { rawPrompt: null, rawSettings: {} } };
    },
  });
}

async function makeRuntime(m: any, config: Partial<AgentConfig> = {}) {
  const storage = new MemoryStorage();
  const bus = new MemoryBus();
  const queue = new MemoryQueue();
  const kv = new MemoryKv();
  const admin = new MemoryAdminStore();
  const resolved = resolveConfig({ stopPollMs: 20, ...config });
  const runtime = await setupAgentCore({
    storage, bus, queue, kv, admin,
    resolveModel: () => ({ instance: () => m, contextWindow: 128_000 }),
    config: resolved,
  });
  const ports: RuntimePorts = {
    storage: bindStorage(storage, { state: {} }), bus, queue, kv, admin, config: resolved,
    resolveModel: () => ({ instance: () => m, contextWindow: 128_000 }),
  };
  const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
  const states = (threadId: string) =>
    bus.published
      .filter((e) => e.threadId === threadId && e.type === 'STATE_CHANGE')
      .map((e) => (e.payload as any).state);
  return { runtime, chat, storage, bus, queue, kv, admin, ports, states };
}

describe('state changes are compare-and-set', () => {
  it('a finish loses to a stop that landed first', async () => {
    const r = await makeRuntime(model(['ok']));
    const ran = await r.chat.run({ prompt: 'hi' });
    // A worker picked the run up…
    expect(await transition(r.ports, ran.threadId, { from: ['QUEUED'], to: 'RUNNING', runId: ran.runId })).toBe(true);
    // …the user stopped it…
    expect((await r.chat.stop(ran.threadId)).accepted).toBe(true);
    // …and then the worker finished, having read RUNNING before the stop.
    await finalize(r.ports, undefined as never, ran.threadId, {
      state: 'COMPLETED', stopReason: 'completed', tokensUsed: 0,
      attribution: { inputTokens: 0, cachedInputTokens: 0, outputTokens: 0, totalTokens: 0 },
      runId: ran.runId,
    });
    expect((await r.storage.threads.get(ran.threadId))!.state).toBe('CANCELLED');
    expect(await r.kv.get(`agent:state:${ran.threadId}`)).toBe('CANCELLED');
    expect(r.states(ran.threadId)).toEqual(['QUEUED', 'CANCELLED']); // no COMPLETED after the stop
    expect((await r.admin.runs.get(ran.runId!))!.state).toBe('CANCELLED');
  });

  it('a queued run is cancelled and its job does nothing', async () => {
    let calls = 0;
    const r = await makeRuntime(model(['ok'], async () => void calls++));
    const ran = await r.chat.run({ prompt: 'hi' });
    expect((await r.chat.stop(ran.threadId)).accepted).toBe(true);
    await r.runtime.worker.handleJob(r.queue.items[0]!);
    expect(calls).toBe(0);
    expect((await r.storage.threads.get(ran.threadId))!.state).toBe('CANCELLED');
  });

  it('a stopped run parks nothing', async () => {
    const r = await makeRuntime(model(['ok']));
    const ran = await r.chat.run({ prompt: 'hi' });
    await transition(r.ports, ran.threadId, { from: ['QUEUED'], to: 'RUNNING', runId: ran.runId });
    await r.chat.stop(ran.threadId);
    // A tool that was mid-call when the stop landed now tries to park.
    await parkForApproval(r.ports, {
      threadId: ran.threadId, toolCallId: 'c1', toolName: 'send', args: {},
      resume: { agent: 'chat', model: 'gpt-4o', runId: ran.runId },
    } as any);
    expect((await r.storage.threads.get(ran.threadId))!.state).toBe('CANCELLED');
    expect(r.bus.published.some((e) => e.type === 'INPUT_REQUIRED')).toBe(false);
    expect(r.queue.items).toHaveLength(1); // no expiry job (only the original dispatch)
  });

  it("a replaced run's failure spends no attempt", async () => {
    let threadId = '';
    let kv!: MemoryKv;
    // The run is replaced while its model call is in flight, then the call fails.
    const r = await makeRuntime(model(['fail'], async () => {
      await kv.set(runIdKey(threadId), 'a-newer-run');
    }));
    kv = r.kv;
    const ran = await r.chat.run({ prompt: 'hi' });
    threadId = ran.threadId;
    await r.runtime.worker.handleJob(r.queue.items[0]!);
    expect(await r.kv.get(attemptsKey(ran.runId!))).toBeNull(); // no attempt spent
    expect(r.queue.items).toHaveLength(1); // nothing retried
  });

  it('a failed run waits QUEUED for its retry', async () => {
    const r = await makeRuntime(model(['fail', 'ok']), { runRetryBackoffMs: 1 });
    const ran = await r.chat.run({ prompt: 'hi' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);
    expect((await r.storage.threads.get(ran.threadId))!.state).toBe('QUEUED');
    expect(r.states(ran.threadId)).toEqual(['QUEUED', 'RUNNING', 'QUEUED']);
    expect(await r.kv.get(attemptsKey(ran.runId!))).toBe('1'); // counted per run
    await r.runtime.worker.handleJob(r.queue.items[1]!);
    expect((await r.storage.threads.get(ran.threadId))!.state).toBe('COMPLETED');
  });
});
