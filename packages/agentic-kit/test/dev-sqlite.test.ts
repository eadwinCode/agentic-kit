import { describe, expect, it } from 'bun:test';
import { Database } from 'bun:sqlite';
import { simulateReadableStream, tool } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { z } from 'zod';
import { setupAgentCore } from '../src/runtime.js';
import { SqliteStorage } from '../src/adapters/sqlite.js';
import { InlineQueue } from '../src/adapters/inline.js';
import { MemoryBus, MemoryKv } from '../src/adapters/memory.js';
import { SqliteAdminStore } from '../src/admin/sqlite.js';
import { markRequiresConfirmation } from '../src/core/engine.js';
import type { AgentConfig } from '../src/core/types.js';

const finish = (r: string) =>
  ({ type: 'finish', finishReason: r, usage: { promptTokens: 10, completionTokens: 5 } }) as
    LanguageModelV1StreamPart;
const stream = (chunks: LanguageModelV1StreamPart[]) => ({
  stream: simulateReadableStream({ chunks }),
  rawCall: { rawPrompt: null, rawSettings: {} },
});
const settle = (ms = 60) => new Promise((r) => setTimeout(r, ms));

const echo = () =>
  new MockLanguageModelV1({
    provider: 'mock', modelId: 'mock',
    doStream: async () => stream([{ type: 'text-delta', textDelta: 'hello' }, finish('stop')]),
  });

/** The whole local assembly, spelled out — there is no factory hiding it.
 *  One SQLite file behind both stores, a queue that dispatches in-process, and
 *  bus and kv in memory. */
async function localRuntime(model: any = echo(), config: Partial<AgentConfig> = {}) {
  const db = new Database(':memory:');
  const queue = new InlineQueue();
  const runtime = await setupAgentCore({
    storage: new SqliteStorage(db),
    admin: new SqliteAdminStore(db),
    bus: new MemoryBus(),
    kv: new MemoryKv(),
    queue,
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
    config,
  });
  // The queue and the worker each need the other, so the queue is attached
  // once the core exists. Nothing dispatches until this happens.
  queue.bind((job) => runtime.worker.handleJob(job));
  return runtime;
}

const devRuntime = (model: any = echo()) => localRuntime(model);

describe('SqliteStorage', () => {
  const store = () => new SqliteStorage(new Database(':memory:'));

  it('orders messages by insertion, not by clock', async () => {
    const s = store();
    const t = await s.threads.create({ model: 'gpt-4o' });
    // Several messages land inside one millisecond; createdAt cannot order them.
    const ids: string[] = [];
    for (const role of ['user', 'assistant', 'tool', 'assistant'] as const) {
      ids.push((await s.messages.append(t.id, { role, content: role })).id);
    }
    expect((await s.messages.list(t.id)).map((m) => m.id)).toEqual(ids);
  });

  it('scopes messages by agent, distinguishing null from absent', async () => {
    const s = store();
    const t = await s.threads.create({ model: 'gpt-4o' });
    await s.messages.append(t.id, { role: 'user', content: 'main' });
    await s.messages.append(t.id, { role: 'user', content: 'child', agentId: 'sub-1' });

    expect(await s.messages.list(t.id)).toHaveLength(2);           // unscoped
    expect(await s.messages.list(t.id, { agentId: null })).toHaveLength(1);
    expect(await s.messages.list(t.id, { agentId: 'sub-1' })).toHaveLength(1);
  });

  it('deletes a message and its suffix', async () => {
    const s = store();
    const t = await s.threads.create({ model: 'gpt-4o' });
    const a = await s.messages.append(t.id, { role: 'user', content: 'a' });
    const b = await s.messages.append(t.id, { role: 'assistant', content: 'b' });
    await s.messages.append(t.id, { role: 'user', content: 'c' });

    expect(await s.messages.deleteFrom(t.id, b.id)).toBe(2);
    expect((await s.messages.list(t.id)).map((m) => m.id)).toEqual([a.id]);
    expect(await s.messages.deleteFrom(t.id, b.id)).toBe(0); // already gone
  });

  it('claimState is a compare-and-set: exactly one caller wins', async () => {
    const s = store();
    const t = await s.threads.create({ model: 'gpt-4o' });
    await s.threads.setState(t.id, 'WAITING_FOR_INPUT');
    expect(await s.threads.claimState(t.id, 'WAITING_FOR_INPUT', 'RUNNING')).toBe(true);
    expect(await s.threads.claimState(t.id, 'WAITING_FOR_INPUT', 'RUNNING')).toBe(false);
  });

  it('deleting a thread takes everything that follows it', async () => {
    const s = store();
    const t = await s.threads.create({ model: 'gpt-4o' });
    await s.messages.append(t.id, { role: 'user', content: 'a' });
    await s.events.append(t.id, { threadId: t.id, seq: 1, type: 'CHUNK', payload: {}, createdAt: new Date() } as any);
    await s.usage.record(t.id, { agentId: null, inputTokens: 1, cachedInputTokens: 0, outputTokens: 1, totalTokens: 2 });
    await s.threads.delete(t.id);

    expect(await s.threads.get(t.id)).toBeNull();
    expect(await s.messages.list(t.id)).toEqual([]);
    expect(await s.events.listSince(t.id, -1)).toEqual([]);
    expect((await s.usage.total(t.id)).totalTokens).toBe(0);
  });
});

describe('SqliteAdminStore', () => {
  it('round-trips a run and its steps', async () => {
    const a = new SqliteAdminStore(new Database(':memory:'));
    const run = await a.runs.start({ id: 'r1', threadId: 't1', agent: 'chat', model: 'gpt-4o' });
    expect(run).toMatchObject({ id: 'r1', depth: 0, parentRunId: null, state: 'RUNNING' });

    // A nested run is the same record, one level down (§2.7).
    await a.runs.start({
      id: 'r2', threadId: 't1', agent: 'kid', model: 'gpt-4o', depth: 1, parentRunId: 'r1',
    });
    await a.runs.patch('r1', {
      state: 'COMPLETED', stopReason: 'completed', endedAt: new Date(),
      durationMs: 42, steps: 3, totalTokens: 90, result: { text: 'done' },
    });

    const got = (await a.runs.get('r1'))!;
    expect(got).toMatchObject({ state: 'COMPLETED', durationMs: 42, steps: 3, totalTokens: 90 });
    expect(got.endedAt).toBeInstanceOf(Date);
    expect(got.result).toEqual({ text: 'done' });
    expect((await a.runs.listByThread('t1')).map((r) => r.depth).sort()).toEqual([0, 1]);
    expect(await a.runs.countByState()).toMatchObject({ COMPLETED: 1, RUNNING: 1 });

    await a.steps.record({
      runId: 'r1', agentId: null, index: 1, durationMs: 12, finishReason: 'stop',
      inputTokens: 10, cachedInputTokens: 0, outputTokens: 5, totalTokens: 15,
      tools: ['sendEmail'],
    });
    const steps = await a.steps.listByRun('r1');
    expect(steps).toHaveLength(1);
    expect(steps[0]).toMatchObject({ index: 1, totalTokens: 15, tools: ['sendEmail'] });
    expect(steps[0]!.at).toBeInstanceOf(Date);
  });
});

describe('a locally assembled runtime', () => {
  it('runs end to end with nothing to stand up first', async () => {
    const runtime = await devRuntime();
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });

    const ran = await chat.run({ prompt: 'hi' });
    // The dev queue really dispatches — nobody drives the worker here.
    await settle(150);

    const snapshot = (await runtime.getThreadSnapshot(ran.threadId))!;
    expect(snapshot.thread.state).toBe('COMPLETED');
    expect(snapshot.messages.map((m) => m.role)).toEqual(['user', 'assistant']);

    const detail = (await runtime.admin.getRun(ran.runId!))!;
    expect(detail.run).toMatchObject({ state: 'COMPLETED', steps: 1, totalTokens: 15 });
    expect(detail.steps).toHaveLength(1);

    const stats = await runtime.admin.stats();
    expect(stats.total).toBe(1);
    expect(stats.byState.COMPLETED).toBe(1);
  }, 10_000);

  it('honours delayed dispatch, so a park expires on its own', async () => {
    const parking = new MockLanguageModelV1({
      provider: 'mock', modelId: 'parking',
      doStream: async ({ prompt }: any) => {
        const answered = (prompt ?? []).some((m: any) => m.role === 'tool');
        return answered
          ? stream([{ type: 'text-delta', textDelta: 'moved on' }, finish('stop')])
          : stream([
              { type: 'tool-call', toolCallType: 'function', toolCallId: 'c1',
                toolName: 'sendEmail', args: JSON.stringify({ to: 'a@b.c' }) } as any,
              finish('tool-calls'),
            ]);
      },
    });
    // A TTL short enough to observe: the park schedules its own expiry.
    const runtime = await localRuntime(parking, { hitlTtlMs: 50, reclaimGraceMs: 0 });
    const chat = runtime.createStreamTextAgent({
      name: 'chat', model: 'gpt-4o',
      tools: {
        sendEmail: markRequiresConfirmation(
          tool({ parameters: z.object({ to: z.string() }), execute: async () => ({ sent: true }) }),
        ),
      },
    });

    const ran = await chat.run({ prompt: 'send it' });
    await settle(120);
    expect((await runtime.getThreadSnapshot(ran.threadId))!.thread.state)
      .toBe('WAITING_FOR_INPUT');

    // Nobody watching, nobody answering — the scheduled expiry resolves it.
    // The delay is whole seconds (Math.ceil), so the soonest it fires is 1s.
    await settle(1_400);
    expect((await runtime.getThreadSnapshot(ran.threadId))!.thread.state).toBe('COMPLETED');
  }, 10_000);
});
