import { describe, expect, it } from 'bun:test';
import { simulateReadableStream } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { RunSlots } from '../src/core/subagent.js';
import { resolveConfig, type AgentConfig } from '../src/core/types.js';

// Workstream F: the subagent cap is per run and per depth, a wait for a slot
// ends on a stop, and a child that was stopped or cut short is never taken
// for finished. The same cases run in the Go package
// (subagent_slots_test.go).

interface Step {
  text?: string;
  calls?: Array<{ id: string; instructions: string }>;
  delayMs?: number;
  noFinish?: boolean;
}

/** Answers by who is asking rather than by call order: nested runs call it in
 *  parallel, so their order is not fixed. */
function routedModel(route: (brief: string, answered: boolean) => Step) {
  return new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock-routed',
    doStream: async ({ prompt, abortSignal }: any) => {
      const first = (prompt as any[]).find((m) => m.role === 'user');
      const brief = (first?.content ?? []).map((p: any) => p.text ?? '').join('');
      const answered = (prompt as any[]).some((m) => m.role === 'tool');
      const s = route(brief, answered);
      if (s.delayMs) {
        await new Promise<void>((resolve, reject) => {
          const timer = setTimeout(resolve, s.delayMs);
          abortSignal?.addEventListener('abort', () => {
            clearTimeout(timer);
            reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
          });
        });
      }
      const chunks: LanguageModelV1StreamPart[] = [];
      if (s.text) chunks.push({ type: 'text-delta', textDelta: s.text });
      for (const c of s.calls ?? []) {
        chunks.push({
          type: 'tool-call', toolCallType: 'function', toolCallId: c.id, toolName: 'spawnSubagent',
          args: JSON.stringify({ name: 'helper', instructions: c.instructions }),
        });
      }
      if (!s.noFinish) {
        chunks.push({
          type: 'finish',
          finishReason: s.calls?.length ? 'tool-calls' : 'stop',
          usage: { promptTokens: 10, completionTokens: 5 },
        });
      }
      return { stream: simulateReadableStream({ chunks }), rawCall: { rawPrompt: null, rawSettings: {} } };
    },
  });
}

async function routedRuntime(route: (brief: string, answered: boolean) => Step, config: Partial<AgentConfig> = {}) {
  const model = routedModel(route);
  const storage = new MemoryStorage();
  const bus = new MemoryBus();
  const queue = new MemoryQueue();
  const admin = new MemoryAdminStore();
  const runtime = await setupAgentCore({
    storage, bus, queue, kv: new MemoryKv(), admin,
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
    config: resolveConfig({ stopPollMs: 5, runRetryBackoffMs: 1, ...config }),
  });
  const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o', subagents: true });
  const events = (threadId: string, type: string) => bus.published.filter((e) => e.threadId === threadId && e.type === type);
  const state = async (threadId: string) => (await storage.threads.get(threadId))!.state;
  return { runtime, chat, queue, admin, events, state };
}

describe('the subagent cap (§2.7)', () => {
  it('nested spawns at the default limit do not deadlock', async () => {
    const r = await routedRuntime((brief, answered) => {
      if (brief === 'go' && !answered) {
        // The parent fills every slot at depth 1.
        return { calls: [{ id: 's1', instructions: 'child' }, { id: 's2', instructions: 'child' }, { id: 's3', instructions: 'child' }] };
      }
      if (brief === 'go') return { text: 'parent done' };
      // Each child waits a moment before it spawns, so all three hold their
      // slots before any grandchild asks for one.
      if (brief === 'child' && !answered) return { delayMs: 50, calls: [{ id: 'g', instructions: 'grandchild' }] };
      if (brief === 'child') return { text: 'child done' };
      return { text: 'grandchild done' };
    });
    const ran = await r.chat.run({ prompt: 'go' });
    const finished = r.runtime.worker.handleJob(r.queue.items.shift()!);
    const outcome = await Promise.race([
      finished.then(() => 'done'),
      new Promise((resolve) => setTimeout(() => resolve('deadlocked'), 5_000)),
    ]);
    expect(outcome).toBe('done');
    expect(await r.state(ran.threadId)).toBe('COMPLETED');
    expect(r.events(ran.threadId, 'SUBAGENT_COMPLETED')).toHaveLength(6); // 3 children, 3 grandchildren
  }, 10_000);

  it('slots are per depth', async () => {
    const slots = new RunSlots(1);
    const release = await slots.acquire(1);
    const deeper = await Promise.race([
      slots.acquire(2).then(() => 'got it'),
      new Promise((resolve) => setTimeout(() => resolve('blocked'), 100)),
    ]);
    expect(deeper).toBe('got it');
    release();
  });

  it('a wait for a slot ends when the run is stopped', async () => {
    const slots = new RunSlots(1);
    const release = await slots.acquire(1);
    const stop = new AbortController();
    setTimeout(() => stop.abort(new Error('stopped')), 20);
    await expect(slots.acquire(1, stop.signal)).rejects.toThrow('stopped');
    release();
  });
});

describe('a child that did not finish (§2.7)', () => {
  it('a child cut off by a stop is recorded cancelled', async () => {
    let childStarted = false;
    const r = await routedRuntime((brief, answered) => {
      if (brief === 'go' && !answered) return { calls: [{ id: 's1', instructions: 'child' }] };
      if (brief === 'go') return { text: 'parent done' };
      childStarted = true;
      return { text: 'never', delayMs: 2_000 };
    });
    const ran = await r.chat.run({ prompt: 'go' });
    const running = r.runtime.worker.handleJob(r.queue.items.shift()!);
    while (!childStarted) await new Promise((resolve) => setTimeout(resolve, 5));
    await r.chat.stop(ran.threadId);
    await running;
    expect(r.events(ran.threadId, 'SUBAGENT_COMPLETED')).toHaveLength(0);
    const childId = (r.events(ran.threadId, 'SUBAGENT_STARTED')[0]!.payload as any).agentId;
    expect((await r.admin.runs.get(childId))!.state).toBe('CANCELLED');
  });

  it('a child whose stream ends without a finish fails', async () => {
    const r = await routedRuntime((brief, answered) => {
      if (brief === 'go' && !answered) return { calls: [{ id: 's1', instructions: 'child' }] };
      if (brief === 'go') return { text: 'parent done' };
      return { text: 'half an answer', noFinish: true };
    });
    const ran = await r.chat.run({ prompt: 'go' });
    await r.runtime.worker.handleJob(r.queue.items.shift()!);
    expect(await r.state(ran.threadId)).toBe('COMPLETED'); // the parent goes on
    const failed = r.events(ran.threadId, 'SUBAGENT_FAILED');
    expect(failed).toHaveLength(1);
    expect((failed[0]!.payload as any).error).toContain('without a finish');
    expect(r.events(ran.threadId, 'SUBAGENT_COMPLETED')).toHaveLength(0); // partial text is not a result
  });
});

describe('the main agent (§2.8)', () => {
  it('a stream cut without a finish is not completed', async () => {
    let calls = 0;
    const r = await routedRuntime(() => (calls++ === 0 ? { text: 'part', noFinish: true } : { text: 'whole' }));
    const ran = await r.chat.run({ prompt: 'hi' });
    await r.runtime.worker.handleJob(r.queue.items.shift()!);
    expect(await r.state(ran.threadId)).not.toBe('COMPLETED'); // a step that never finished
    await r.runtime.worker.handleJob(r.queue.items.shift()!);
    expect(await r.state(ran.threadId)).toBe('COMPLETED'); // the retry completes it
  });
});
