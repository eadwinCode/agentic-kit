import { describe, expect, it } from 'bun:test';
import { simulateReadableStream, tool } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { z } from 'zod';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { markRequiresConfirmation } from '../src/core/engine.js';
import { runLockKey } from '../src/core/lease.js';
import { attemptsKey, runIdKey } from '../src/core/keys.js';
import { DEFAULT_CONFIG, resolveConfig, type AgentConfig } from '../src/core/types.js';
import { PRIORITY_LOW } from '../src/ports/queue.js';
import type { RunFinishInfo } from '../src/ports/runtime.js';
import { subagents } from './stream-helpers.js';

// Workstream K: the features the Go runtime had and this one did not, under
// the same test names as their Go cases (spec_hooks_test.go,
// queue_hardening_test.go, parity_test.go).

interface Step {
  text?: string;
  calls?: Array<{ id: string; name: string; args?: unknown }>;
  delayMs?: number;
  error?: string;
}

/** A model that answers from a script, in call order, and keeps what each
 *  call was sent. */
function scripted(steps: Step[]) {
  const calls: Array<{ prompt: any[]; tools: any[] }> = [];
  const model = new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock-scripted',
    doStream: async ({ prompt, mode, abortSignal }: any) => {
      calls.push({ prompt, tools: mode?.tools ?? [] });
      const s = steps[Math.min(calls.length - 1, steps.length - 1)]!;
      if (s.delayMs) {
        await new Promise<void>((resolve, reject) => {
          const timer = setTimeout(resolve, s.delayMs);
          abortSignal?.addEventListener('abort', () => {
            clearTimeout(timer);
            reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
          });
        });
      }
      if (s.error) throw new Error(s.error);
      const chunks: LanguageModelV1StreamPart[] = [];
      if (s.text) chunks.push({ type: 'text-delta', textDelta: s.text });
      for (const c of s.calls ?? []) {
        chunks.push({
          type: 'tool-call', toolCallType: 'function', toolCallId: c.id, toolName: c.name,
          args: JSON.stringify(c.args ?? {}),
        });
      }
      chunks.push({
        type: 'finish', finishReason: s.calls?.length ? 'tool-calls' : 'stop',
        usage: { promptTokens: 10, completionTokens: 5 },
      });
      return { stream: simulateReadableStream({ chunks }), rawCall: { rawPrompt: null, rawSettings: {} } };
    },
  });
  /** The system prompt one call was sent. */
  const systemOf = (i: number) =>
    calls[i]!.prompt.filter((m) => m.role === 'system').map((m) => m.content).join('');
  return { model, calls, systemOf };
}

async function harness(steps: Step[], config: Partial<AgentConfig> = {}) {
  const s = scripted(steps);
  const storage = new MemoryStorage();
  const bus = new MemoryBus();
  const queue = new MemoryQueue();
  const kv = new MemoryKv();
  const admin = new MemoryAdminStore();
  const resolved: string[] = [];
  const runtime = await setupAgentCore({
    storage, bus, queue, kv, admin,
    resolveModel: (name) => {
      resolved.push(name);
      return { instance: () => s.model, contextWindow: 128_000 };
    },
    config: resolveConfig({ stopPollMs: 5, runRetryBackoffMs: 0, promptCaching: false, ...config }),
  });
  const next = async (options?: { signal?: AbortSignal }) => {
    const job = queue.items.shift();
    queue.delays.shift();
    if (!job) throw new Error('no job queued');
    await runtime.worker.handleJob(job, options);
    return job;
  };
  const drain = () => queue.drain((job) => runtime.worker.handleJob(job).then(() => undefined));
  const events = (threadId: string, type: string) =>
    bus.published.filter((e) => e.threadId === threadId && e.type === type);
  const lastTerminal = (threadId: string) =>
    events(threadId, 'STATE_CHANGE')
      .map((e) => e.payload as Record<string, any>)
      .filter((p) => ['COMPLETED', 'FAILED', 'CANCELLED'].includes(p.state))
      .at(-1);
  const state = async (threadId: string) => (await storage.threads.get(threadId))!.state;
  const messages = (threadId: string) => storage.messages.store.get(threadId) ?? [];
  return { ...s, runtime, storage, bus, queue, kv, admin, resolved, next, drain, events, lastTerminal, state, messages };
}

const ping = tool({ parameters: z.object({}), execute: async () => 'pong' });
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

describe('run input (§2.1)', () => {
  it('accepts a caller run id and refuses its reuse', async () => {
    const h = await harness([{ text: 'ok' }]);
    const chat = h.runtime.createStreamTextAgent({ name: 'chat' });
    const ran = await chat.run({ prompt: 'one', runId: 'run-abc' });
    expect(ran.runId).toBe('run-abc');
    expect(h.queue.items[0]!.runId).toBe('run-abc');
    expect(await h.kv.get(runIdKey(ran.threadId))).toBe('run-abc');
    expect(await h.admin.runs.get('run-abc')).toBeTruthy();
    await h.next();
    expect(h.lastTerminal(ran.threadId)?.state).toBe('COMPLETED');

    const before = h.messages(ran.threadId).length;
    const again = await chat.run({ threadId: ran.threadId, prompt: 'two', runId: 'run-abc' });
    expect(again.accepted).toBe(false);
    expect(again.error).toBe('Run id already used');
    expect(h.messages(ran.threadId)).toHaveLength(before); // nothing was written
    expect(h.queue.items).toHaveLength(0); // nothing was queued
  });

  it('max steps caps below the config and never above', async () => {
    const looping = { calls: [{ id: 'c1', name: 'ping' }] };
    const h = await harness([looping, looping, looping, looping]);
    const chat = h.runtime.createStreamTextAgent({ name: 'chat', tools: { ping } });
    const ran = await chat.run({ prompt: 'loop', maxSteps: 2 });
    expect(h.queue.items[0]!.maxSteps).toBe(2);
    await h.next();
    expect(h.calls).toHaveLength(2); // two round trips
    expect(h.lastTerminal(ran.threadId)).toMatchObject({ state: 'COMPLETED', stopReason: 'max_steps' });

    // Above the config it is clamped, and the config's ceiling holds.
    await chat.run({ prompt: 'loop', maxSteps: 1_000 });
    expect(h.queue.items[0]!.maxSteps).toBe(DEFAULT_CONFIG.maxSteps);
    expect((await chat.run({ prompt: 'loop', maxSteps: -1 })).accepted).toBe(false);
  });

  it('attachments become image parts the model sees', async () => {
    const h = await harness([{ text: 'a cat' }]);
    const chat = h.runtime.createStreamTextAgent({ name: 'chat' });
    const ran = await chat.run({
      prompt: 'what is this?',
      attachments: [{ url: 'https://cdn.example/cat.png', mediaType: 'image/png' }],
    });
    const parts = h.messages(ran.threadId)[0]!.content as any[];
    expect(parts).toEqual([
      { type: 'text', text: 'what is this?' },
      { type: 'image', image: 'https://cdn.example/cat.png', mimeType: 'image/png' },
    ]);
    expect(JSON.stringify(h.events(ran.threadId, 'MESSAGE_APPENDED')[0]!.payload)).toContain('cat.png');

    await h.next();
    const image = h.calls[0]!.prompt.flatMap((m: any) => (Array.isArray(m.content) ? m.content : []))
      .find((p: any) => p.type === 'image');
    expect(image).toBeTruthy(); // the model saw an image part
    expect(String(image.image)).toContain('cat.png');
    expect(image.mimeType).toBe('image/png');
    expect(h.lastTerminal(ran.threadId)?.state).toBe('COMPLETED');
  });
});

describe('spec hooks (§3.1)', () => {
  it('system fn builds the prompt per step from the run state', async () => {
    const h = await harness([{ calls: [{ id: 'c1', name: 'ping' }] }, { text: 'done' }]);
    let built = 0;
    const chat = h.runtime.createStreamTextAgent({
      name: 'chat', system: 'static persona', tools: { ping },
      systemFn: (threadId, state) => `org=${state.orgId} thread=${threadId} step=${++built}`,
    });
    const ran = await chat.run({ prompt: 'hi', state: { orgId: 'acme' } });
    await h.next();
    expect(h.calls).toHaveLength(2);
    expect(h.systemOf(0)).toBe(`org=acme thread=${ran.threadId} step=1`);
    expect(h.systemOf(1)).toBe(`org=acme thread=${ran.threadId} step=2`);
    expect(h.lastTerminal(ran.threadId)?.state).toBe('COMPLETED');
  });

  it('system fn: an error fails the run', async () => {
    const h = await harness([{ text: 'never' }], { runMaxAttempts: 1 });
    const chat = h.runtime.createStreamTextAgent({
      name: 'chat', systemFn: () => { throw new Error('no project'); },
    });
    const ran = await chat.run({ prompt: 'hi' });
    await h.next();
    expect(await h.state(ran.threadId)).toBe('FAILED');
    expect(h.calls).toHaveLength(0); // the model was never called
  });

  it('prepare step adds ephemeral context that is never persisted', async () => {
    const h = await harness([{ calls: [{ id: 'c1', name: 'ping' }] }, { text: 'done' }]);
    let calls = 0;
    const chat = h.runtime.createStreamTextAgent({
      name: 'chat', tools: { ping },
      prepareStep: (_threadId, _state, messages) => {
        calls++;
        return [...messages, { role: 'user', content: [{ type: 'text', text: 'look at this once' }] }];
      },
    });
    const ran = await chat.run({ prompt: 'hi' });
    await h.next();
    expect(calls).toBe(2); // once per step
    for (const c of h.calls) expect(JSON.stringify(c.prompt.at(-1))).toContain('look at this once');
    expect(JSON.stringify(h.messages(ran.threadId))).not.toContain('look at this once');
    expect(h.lastTerminal(ran.threadId)?.state).toBe('COMPLETED');
  });
});

describe('subagent profiles (§2.7)', () => {
  it('a profile gives the child its own persona, tools and model', async () => {
    const h = await harness([
      { calls: [{ id: 's1', name: 'spawnSubagent', args: { name: 'researcher', instructions: 'find it' } }] },
      { calls: [{ id: 'l1', name: 'lookup', args: { q: 'x' } }] }, // the child uses its own tool
      { text: 'child: found' },
      { text: 'parent: done' },
    ]);
    const looked: string[] = [];
    let persona = 0;
    const chat = h.runtime.createStreamTextAgent({
      name: 'chat', model: 'gpt-4o',
      subagents: {
        profiles: {
          researcher: {
            description: 'finds facts', model: 'gpt-4o-mini', maxSteps: 3,
            systemFn: (_t, state) => { persona++; return `You are the researcher for ${state.orgId}.`; },
            tools: {
              lookup: tool({
                parameters: z.object({ q: z.string() }),
                execute: async ({ q }) => { looked.push(q); return { found: true }; },
              }),
            },
          },
          writer: { description: 'writes copy', system: 'You write.' },
        },
      },
    });
    const ran = await chat.run({ prompt: 'go', state: { orgId: 'acme' } });
    await h.next();
    expect(h.lastTerminal(ran.threadId)?.state).toBe('COMPLETED');
    expect(looked).toEqual(['x']); // the profile's tool ran
    expect(h.systemOf(1)).toBe('You are the researcher for acme.');
    expect(h.systemOf(2)).toBe('You are the researcher for acme.');
    expect(persona).toBe(2); // built once per child step
    const spawn = h.calls[0]!.tools.find((t: any) => t.name === 'spawnSubagent');
    expect(spawn.description).toContain('researcher (finds facts)');
    expect(spawn.description).toContain('writer (writes copy)');
    const childId = (await subagents(h.runtime.ports(), ran.threadId)).started[0]!.subagentId;
    expect(await h.admin.runs.get(childId)).toMatchObject({ model: 'gpt-4o-mini', agent: 'researcher' });
  });

  it('an unknown profile is reported to the model', async () => {
    const h = await harness([
      { calls: [{ id: 's1', name: 'spawnSubagent', args: { name: 'nobody', instructions: '?' } }] },
      { text: 'parent: ok then' },
    ]);
    const chat = h.runtime.createStreamTextAgent({
      name: 'chat', subagents: { profiles: { researcher: { system: 'You research.' } } },
    });
    const ran = await chat.run({ prompt: 'go' });
    await h.next();
    expect(h.lastTerminal(ran.threadId)?.state).toBe('COMPLETED');
    expect(h.events(ran.threadId, 'SUBAGENT_STARTED')).toHaveLength(0); // nothing was spawned
    const result = JSON.stringify(h.messages(ran.threadId)[2]!.content);
    expect(result).toContain('Unknown subagent \\"nobody\\"');
    expect(result).toContain('researcher');
  });

  it("a profile tool parks and resumes with the profile's tool", async () => {
    const h = await harness([
      { calls: [{ id: 's1', name: 'spawnSubagent', args: { name: 'mailer', instructions: 'send it' } }] },
      { calls: [{ id: 'd1', name: 'sendEmail', args: { to: 'a@b.c' } }] },
      { text: 'child: sent' },
      { text: 'parent: done' },
    ]);
    const sent: string[] = [];
    const chat = h.runtime.createStreamTextAgent({
      name: 'chat',
      subagents: {
        profiles: {
          mailer: {
            system: 'You mail.',
            tools: {
              sendEmail: markRequiresConfirmation(tool({
                parameters: z.object({ to: z.string() }),
                execute: async ({ to }) => { sent.push(to); return { sent: true }; },
              })),
            },
          },
        },
      },
    });
    const ran = await chat.run({ prompt: 'go' });
    await h.next();
    expect(await h.state(ran.threadId)).toBe('WAITING_FOR_INPUT');
    expect(sent).toEqual([]);
    await h.runtime.hitl.respond({ threadId: ran.threadId, toolCallId: 'd1', approved: true });
    await h.next();
    expect(sent).toEqual(['a@b.c']); // the profile's tool ran on approval
    expect(h.lastTerminal(ran.threadId)?.state).toBe('COMPLETED');
  });
});

describe('overload (§2.8)', () => {
  it('a full queue refuses a new run before writing anything', async () => {
    const h = await harness([{ text: 'ok' }], { maxQueueDepth: 1 });
    const chat = h.runtime.createStreamTextAgent({ name: 'chat' });
    await chat.run({ prompt: 'one' });
    const thread = await h.storage.threads.create({ model: 'gpt-4o' });
    const res = await chat.run({ threadId: thread.id, prompt: 'two' });
    expect(res.accepted).toBe(false);
    expect(res.reason).toBe('queue_full');
    expect(await h.state(thread.id)).toBe('IDLE'); // untouched
    expect(h.messages(thread.id)).toHaveLength(0);
    expect(await h.admin.runs.listByThread(thread.id)).toHaveLength(0);
    expect(h.queue.items).toHaveLength(1);
    expect(h.events(thread.id, 'RUN_REFUSED')).toHaveLength(1);
    await h.next();
    expect((await chat.run({ threadId: thread.id, prompt: 'two' })).accepted).toBe(true);
  });

  it('the adapter cap spares retries', async () => {
    const h = await harness([{ error: 'boom' }, { text: 'ok' }]);
    h.queue.maxDepth = 1;
    const chat = h.runtime.createStreamTextAgent({ name: 'chat' });
    const ran = await chat.run({ prompt: 'one' });
    await h.next(); // fails once: the retry is queued although the cap is 1
    expect(h.queue.items).toHaveLength(1);
    expect(h.queue.items[0]!.kind).toBe('retry');
    const thread = await h.storage.threads.create({ model: 'gpt-4o' });
    const res = await chat.run({ threadId: thread.id, prompt: 'two' });
    expect(res.accepted).toBe(false);
    expect(res.reason).toBe('queue_full');
    await h.drain();
    expect(h.lastTerminal(ran.threadId)?.state).toBe('COMPLETED');
  });

  it('the ticket carries the tenant and housekeeping queues behind users', async () => {
    const h = await harness([{ calls: [{ id: 'c1', name: 'wipe' }] }], { hitlTtlMs: 60 * 60_000 });
    const wipe = markRequiresConfirmation(tool({ parameters: z.object({}), execute: async () => 'wiped' }));
    const chat = h.runtime.createStreamTextAgent({ name: 'chat', tools: { wipe } });
    await chat.run({ prompt: 'delete', partitionKey: 'team-a' });
    const job = h.queue.items[0]!;
    expect(job.partitionKey).toBe('team-a');
    expect(job.kind).toBeUndefined(); // a fresh dispatch
    expect(job.dispatchedAt && job.enqueuedAt).toBeTruthy();
    expect(h.queue.priorityOf(job)).toBe(0);
    await h.next(); // parks and schedules its expiry
    const expiry = h.queue.items[0]!;
    expect(expiry.kind).toBe('expiry');
    expect(h.queue.priorityOf(expiry)).toBe(PRIORITY_LOW);
    expect(h.queue.keyOf(expiry)).toBe('hitl-expiry:c1');
    expect(expiry.dispatchedAt).toBe(job.dispatchedAt); // keeps the run's place in line
  });

  it('a job that waited past max queue wait fails instead of running', async () => {
    const h = await harness([{ text: 'never' }], { maxQueueWaitMs: 20 });
    const chat = h.runtime.createStreamTextAgent({ name: 'chat' });
    const ran = await chat.run({ prompt: 'go' });
    await sleep(40);
    await h.next();
    const term = h.lastTerminal(ran.threadId)!;
    expect(term.state).toBe('FAILED');
    expect(term.error).toContain('waited');
    expect(h.calls).toHaveLength(0);
    expect(h.queue.items).toHaveLength(0);
  });
});

describe('deadlines (§2.8)', () => {
  it('a step past step timeout fails and is retried', async () => {
    const h = await harness([{ text: 'slow', delayMs: 300 }, { text: 'fast' }], { stepTimeoutMs: 50 });
    const chat = h.runtime.createStreamTextAgent({ name: 'chat' });
    const ran = await chat.run({ prompt: 'go' });
    await h.next();
    expect(h.queue.items).toHaveLength(1);
    expect(h.queue.items[0]!.kind).toBe('retry');
    expect(await h.state(ran.threadId)).toBe('QUEUED');
    await h.next();
    expect(h.lastTerminal(ran.threadId)?.state).toBe('COMPLETED');
    expect(h.calls).toHaveLength(2);
  });

  it('a segment past segment timeout settles failed', async () => {
    const h = await harness([{ text: 'slow', delayMs: 400 }], { segmentTimeoutMs: 60 });
    const settled: RunFinishInfo[] = [];
    const chat = h.runtime.createStreamTextAgent({ name: 'chat', onSettle: (info) => { settled.push(info); } });
    const ran = await chat.run({ prompt: 'go' });
    const started = Date.now();
    await h.next();
    expect(Date.now() - started).toBeLessThan(300); // ends at the deadline
    const term = h.lastTerminal(ran.threadId)!;
    expect(term).toMatchObject({ state: 'FAILED', stopReason: 'timeout' });
    expect(term.error).toContain('longer than');
    const rec = await h.admin.runs.get(ran.runId!);
    expect(rec!.state).toBe('FAILED');
    expect(rec!.settledAt).toBeTruthy();
    expect(settled).toHaveLength(1);
    expect(settled[0]!.state).toBe('FAILED');
    expect(h.queue.items).toHaveLength(0); // not retried
    expect(await h.kv.get(runLockKey(ran.threadId))).toBeNull();
  });
});

describe('billing (§4)', () => {
  it('the pickup check can lower the cap or refuse the run', async () => {
    const stages: string[] = [];
    const h = await harness(
      [{ calls: [{ id: 'c1', name: 'ping' }] }, { calls: [{ id: 'c2', name: 'ping' }] }, { text: 'done' }],
      {
        billingPreCheck: async (check) => {
          stages.push(check.stage);
          if (check.stage === 'pickup') {
            expect(check.runId && check.budget).toBeTruthy();
            check.budget!.maxSteps = 1;
          }
          return { ok: true };
        },
      },
    );
    const chat = h.runtime.createStreamTextAgent({ name: 'chat', tools: { ping } });
    const ran = await chat.run({ prompt: 'go' });
    await h.next();
    expect(stages).toEqual(['dispatch', 'pickup']);
    expect(h.lastTerminal(ran.threadId)?.stopReason).toBe('max_steps'); // the lowered cap applied
    expect(h.calls).toHaveLength(1);

    const r = await harness([{ text: 'never' }], {
      billingPreCheck: async (check) =>
        check.stage === 'pickup' ? { ok: false, error: 'out of credits' } : { ok: true },
    });
    const settled: RunFinishInfo[] = [];
    const chat2 = r.runtime.createStreamTextAgent({ name: 'chat', onSettle: (info) => { settled.push(info); } });
    const ran2 = await chat2.run({ prompt: 'go' });
    await r.next();
    expect(r.lastTerminal(ran2.threadId)).toMatchObject({ state: 'FAILED', error: 'out of credits' });
    expect(r.calls).toHaveLength(0);
    expect(r.events(ran2.threadId, 'RUN_REFUSED')).toHaveLength(1);
    expect(settled).toHaveLength(1);
    expect(r.queue.items).toHaveLength(0);
  });
});

describe('lost and dead jobs (§2.8)', () => {
  it('a thread with no lock and no job is redispatched', async () => {
    const h = await harness([{ text: 'ok' }], { runLockLeaseSeconds: 1 });
    const chat = h.runtime.createStreamTextAgent({ name: 'chat' });
    const ran = await chat.run({ prompt: 'go', state: { tenant: 'acme' } });
    h.queue.items.shift(); // the queue lost the job
    h.queue.delays.shift();
    let report = await h.runtime.reclaimStuckRuns(0);
    expect(report.redispatched).toBe(0); // too soon: a run just accepted has no lock and no job for a moment
    await sleep(1_050);
    report = await h.runtime.reclaimStuckRuns(0);
    expect(report.redispatched).toBe(1);
    expect(h.queue.items).toHaveLength(1);
    expect(h.queue.items[0]!.kind).toBe('reclaim');
    expect(h.queue.keyOf(h.queue.items[0]!)).toBe(`reclaim:${ran.runId}`);
    expect(h.queue.items[0]!.state).toEqual({ tenant: 'acme' });
    await h.drain();
    expect(h.lastTerminal(ran.threadId)?.state).toBe('COMPLETED');
  });

  it('the dead job handler fails the run it belonged to', async () => {
    const settled: RunFinishInfo[] = [];
    const h = await harness([{ text: 'ok' }]);
    const chat = h.runtime.createStreamTextAgent({ name: 'chat', onSettle: (info) => { settled.push(info); } });
    const ran = await chat.run({ prompt: 'go' });
    const job = h.queue.items.shift()!;
    h.queue.delays.shift();
    await h.runtime.worker.handleDeadJob(job, 5, new Error('boom'));
    expect(await h.state(ran.threadId)).toBe('FAILED');
    expect(await h.kv.get(`agent:state:${ran.threadId}`)).toBe('FAILED');
    const rec = await h.admin.runs.get(ran.runId!);
    expect(rec!.state).toBe('FAILED');
    expect(rec!.error).toContain('dropped by the queue');
    expect(rec!.error).toContain('boom');
    expect(settled).toHaveLength(1);
    expect(await h.kv.get(runLockKey(ran.threadId))).toBeNull();

    // The thread accepts a new run, and a stale dead job cannot touch it.
    await chat.run({ threadId: ran.threadId, prompt: 'again' });
    await h.runtime.worker.handleDeadJob(job, 5, new Error('boom'));
    expect(await h.state(ran.threadId)).toBe('QUEUED');
    await h.next();
    expect(h.lastTerminal(ran.threadId)?.state).toBe('COMPLETED');
  });

  it('a cancelled worker requeues the run without spending an attempt', async () => {
    const h = await harness([{ text: 'slow', delayMs: 500 }, { text: 'done' }], { runMaxAttempts: 1 });
    const chat = h.runtime.createStreamTextAgent({ name: 'chat' });
    const ran = await chat.run({ prompt: 'go' });
    const shutdown = new AbortController();
    const worker = h.next({ signal: shutdown.signal });
    await sleep(60);
    shutdown.abort();
    await worker; // hands the job back cleanly
    expect(h.queue.items).toHaveLength(1);
    expect(h.queue.items[0]!.kind).toBe('retry');
    expect(h.queue.delays[0]).toBeUndefined(); // at once
    expect(await h.kv.get(attemptsKey(ran.runId!))).toBeNull(); // no attempt spent
    expect(await h.state(ran.threadId)).toBe('QUEUED');
    await h.next(); // with runMaxAttempts 1, a counted attempt would have failed it here
    expect(h.lastTerminal(ran.threadId)?.state).toBe('COMPLETED');
  });
});

describe('config and budgets', () => {
  it('a partial config keeps the defaults', () => {
    const config = resolveConfig({ maxSteps: 50 });
    expect(config.maxSteps).toBe(50);
    expect(config.stopPollMs).toBe(DEFAULT_CONFIG.stopPollMs);
    expect(config.runLockLeaseSeconds).toBe(DEFAULT_CONFIG.runLockLeaseSeconds);
    expect(config.compactionModel).toBe(DEFAULT_CONFIG.compactionModel);
  });

  it('a budget of zero means no cap from this level', async () => {
    const looping = { calls: [{ id: 'c1', name: 'ping' }] };
    const h = await harness([looping, looping, { text: 'done' }]);
    // The spec's budget stands under a run that sends 0.
    const chat = h.runtime.createStreamTextAgent({ name: 'chat', tools: { ping }, tokenBudget: 20 });
    const ran = await chat.run({ prompt: 'go', tokenBudget: 0 });
    await h.next();
    expect(h.lastTerminal(ran.threadId)?.stopReason).toBe('token_budget');
    // A negative one is a caller's bug, refused rather than read as no cap.
    await expect(
      chat.executeWithPolicy({ threadId: ran.threadId, model: 'gpt-4o', tokenBudget: -1 }, { maxAttempts: 1 }),
    ).resolves.toBeUndefined();
  });

  it("the runtime's ports are scoped to a run's state", async () => {
    const h = await harness([{ text: 'ok' }]);
    const scoped = h.runtime.ports({ tenant: 'acme' });
    expect(scoped.config.maxSteps).toBe(DEFAULT_CONFIG.maxSteps);
    const thread = await scoped.storage.threads.create({ model: 'gpt-4o' });
    expect(await h.runtime.ports().storage.threads.get(thread.id)).toBeTruthy();
  });
});
