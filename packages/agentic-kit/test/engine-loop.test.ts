import { describe, expect, it } from 'bun:test';
import { tool, simulateReadableStream } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { z } from 'zod';
import { MockLanguageModelV1 } from 'ai/test';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { markRequiresConfirmation } from '../src/core/engine.js';
import { resolveConfig, type AgentConfig } from '../src/core/types.js';
import type { RuntimePorts } from '../src/ports/runtime.js';

interface ScriptedStep {
  text?: string;
  toolCalls?: Array<{ toolCallId: string; toolName: string; args: any }>;
  usage?: [promptTokens: number, completionTokens: number];
}

/** A mock model that plays back one scripted step per SDK round-trip — the
 *  platform loop makes one round-trip per iteration (executeStep, maxSteps: 1).
 *  Implements both flavors: doStream (stream-text) and doGenerate (generate-text). */
function scriptedModel(steps: ScriptedStep[]) {
  let call = 0;
  const nextStep = () => steps[Math.min(call++, steps.length - 1)]!;
  const usageOf = (s: ScriptedStep) => ({
    promptTokens: s.usage?.[0] ?? 10,
    completionTokens: s.usage?.[1] ?? 5,
  });
  return new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock-scripted',
    doStream: async () => {
      const step = nextStep();
      const chunks: LanguageModelV1StreamPart[] = [];
      if (step.text) chunks.push({ type: 'text-delta', textDelta: step.text });
      for (const tc of step.toolCalls ?? []) {
        chunks.push({
          type: 'tool-call',
          toolCallType: 'function',
          toolCallId: tc.toolCallId,
          toolName: tc.toolName,
          args: JSON.stringify(tc.args),
        });
      }
      chunks.push({
        type: 'finish',
        finishReason: (step.toolCalls?.length ?? 0) > 0 ? 'tool-calls' : 'stop',
        usage: usageOf(step),
      });
      return {
        stream: simulateReadableStream({ chunks }),
        rawCall: { rawPrompt: null, rawSettings: {} },
      };
    },
    doGenerate: async () => {
      const step = nextStep();
      return {
        text: step.text ?? '',
        toolCalls: (step.toolCalls ?? []).map((tc) => ({
          toolCallType: 'function' as const,
          toolCallId: tc.toolCallId,
          toolName: tc.toolName,
          args: JSON.stringify(tc.args),
        })),
        finishReason: (step.toolCalls?.length ?? 0) > 0 ? ('tool-calls' as const) : ('stop' as const),
        usage: usageOf(step),
        rawCall: { rawPrompt: null, rawSettings: {} },
      };
    },
  });
}

function makeRuntime(model: any, config: Partial<AgentConfig> = {}) {
  const storage = new MemoryStorage();
  const bus = new MemoryBus();
  const queue = new MemoryQueue();
  const kv = new MemoryKv();
  const deps: RuntimePorts = {
    storage,
    bus,
    queue,
    kv,
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
    config: resolveConfig(config),
  };
  return { deps, runtime: setupAgentCore(deps), storage, bus, queue, kv };
}

const states = (bus: MemoryBus) =>
  bus.published.filter((e) => e.type === 'STATE_CHANGE').map((e) => (e.payload as any).state);
const stripThread = (rows: Array<any>) =>
  rows.map(({ threadId: _t, ...rest }) => rest);
const lastTerminal = (bus: MemoryBus) =>
  bus.published.filter((e) => e.type === 'STATE_CHANGE').at(-1)!;
const roles = (storage: MemoryStorage, threadId: string) =>
  storage.messages.store.get(threadId)!.map((m) => m.role);

describe('engine loop (§2.1, §5.6): platform-owned continuation', () => {
  it('feeds tool results back between single-round-trip steps and persists per step', async () => {
    const executed: string[] = [];
    const { runtime, storage, bus, queue } = makeRuntime(
      scriptedModel([
        { toolCalls: [{ toolCallId: 'call_1', toolName: 'lookup', args: { q: 'x' } }] },
        { text: 'done' },
      ]),
    );
    const chat = runtime.createStreamTextAgent({
      name: 'chat',
      model: 'gpt-4o',
      tools: {
        lookup: tool({
          parameters: z.object({ q: z.string() }),
          execute: async ({ q }) => {
            executed.push(q);
            return { ok: true };
          },
        }),
      },
    });

    const ran = await chat.run({ prompt: 'hi' });
    expect(queue.items).toHaveLength(1);
    await runtime.worker.handleJob(queue.items[0]!);

    expect(executed).toEqual(['x']);
    expect(states(bus)).toEqual(['RUNNING', 'COMPLETED']);
    const terminal = lastTerminal(bus).payload as any;
    expect(terminal.stopReason).toBe('completed');
    expect(terminal.tokensUsed).toBe(30); // 15 per step × 2 steps

    // Per-step persistence: user, assistant(tool-call), tool(result), assistant(text)
    expect(roles(storage, ran.threadId)).toEqual(['user', 'assistant', 'tool', 'assistant']);
    const toolMsg = storage.messages.store.get(ran.threadId)![2]!;
    expect((toolMsg.content as any)[0]).toMatchObject({
      type: 'tool-result',
      toolCallId: 'call_1',
      toolName: 'lookup',
      result: { ok: true },
    });

    // §4 attribution: both steps recorded as one segment row
    expect(stripThread(storage.usage.recorded)).toEqual([
      { agentId: 'chat', inputTokens: 20, cachedInputTokens: 0, outputTokens: 10, totalTokens: 30 },
    ]);
  });

  it('checks the budget BETWEEN steps: the step that crosses the line is kept in full', async () => {
    const executed: string[] = [];
    const { runtime, storage, bus, queue } = makeRuntime(
      scriptedModel([
        { toolCalls: [{ toolCallId: 'call_1', toolName: 'lookup', args: { q: 'x' } }], usage: [100, 20] },
        { text: 'done', usage: [100, 20] },
      ]),
      { tokenBudget: 150 },
    );
    const chat = runtime.createStreamTextAgent({
      name: 'chat',
      model: 'gpt-4o',
      tools: {
        lookup: tool({
          parameters: z.object({ q: z.string() }),
          execute: async ({ q }) => {
            executed.push(q);
            return { ok: true };
          },
        }),
      },
    });

    const ran = await chat.run({ prompt: 'hi' });
    await runtime.worker.handleJob(queue.items[0]!);

    // Step 1 (120 tokens) stayed under budget → step 2 RAN and completed; only
    // step 3 was prevented. Nothing aborted mid-generation.
    expect(executed).toEqual(['x']);
    expect(states(bus)).toEqual(['RUNNING', 'COMPLETED']);
    const terminal = lastTerminal(bus).payload as any;
    expect(terminal.stopReason).toBe('token_budget');
    expect(terminal.tokensUsed).toBe(240);
    expect(roles(storage, ran.threadId)).toEqual(['user', 'assistant', 'tool', 'assistant']);
    const last = storage.messages.store.get(ran.threadId)!.at(-1)!;
    expect((last.content as any)[0].text).toBe('done');
  });

  it('finalizes max_steps when the ceiling is hit with pending tool calls', async () => {
    const { runtime, storage, bus, queue } = makeRuntime(
      scriptedModel([
        { toolCalls: [{ toolCallId: 'call_1', toolName: 'lookup', args: { q: 'x' } }] },
        { toolCalls: [{ toolCallId: 'call_2', toolName: 'lookup', args: { q: 'y' } }] },
      ]),
      { maxSteps: 2, subagentMaxSteps: 2 },
    );
    const chat = runtime.createStreamTextAgent({
      name: 'chat',
      model: 'gpt-4o',
      tools: {
        lookup: tool({
          parameters: z.object({ q: z.string() }),
          execute: async () => ({ ok: true }),
        }),
      },
    });

    const ran = await chat.run({ prompt: 'hi' });
    await runtime.worker.handleJob(queue.items[0]!);

    expect(lastTerminal(bus).payload).toMatchObject({ state: 'COMPLETED', stopReason: 'max_steps' });
    expect(roles(storage, ran.threadId)).toEqual(['user', 'assistant', 'tool', 'assistant', 'tool']);
  });

  it('one-shot flavor publishes TEXT_RESULT and needs no CHUNK stream', async () => {
    const { runtime, bus, queue } = makeRuntime(scriptedModel([{ text: 'answer' }]));
    const agent = runtime.createGenerateTextAgent({ name: 'oneshot', model: 'gpt-4o' });

    const ran = await agent.run({ prompt: 'hi' });
    await runtime.worker.handleJob(queue.items[0]!);

    expect(states(bus)).toEqual(['RUNNING', 'COMPLETED']);
    expect(lastTerminal(bus).payload).toMatchObject({ state: 'COMPLETED', stopReason: 'completed' });
    const textResult = bus.published.find((e) => e.type === 'TEXT_RESULT');
    expect((textResult!.payload as any).text).toBe('answer');
    expect(bus.published.some((e) => e.type === 'CHUNK')).toBe(false);
    expect(ran.accepted).toBe(true);
  });
});

describe('HITL run-segment park (§2.5)', () => {
  function makeHitlRuntime(config: Partial<AgentConfig> = {}) {
    const sent: number[] = [];
    const { runtime, storage, bus, queue, kv } = makeRuntime(
      scriptedModel([
        { toolCalls: [{ toolCallId: 'call_1', toolName: 'sendEmail', args: { to: 'a@b.c' } }] },
        { text: 'sent it' },
      ]),
      config,
    );
    const chat = runtime.createStreamTextAgent({
      name: 'chat',
      model: 'gpt-4o',
      tools: {
        sendEmail: markRequiresConfirmation(
          tool({
            description: 'sends',
            parameters: z.object({ to: z.string() }),
            execute: async () => {
              sent.push(1);
              return { status: 'SENT' };
            },
          }),
        ),
      },
    });
    return { runtime, storage, bus, queue, kv, chat, sent };
  }

  /** run + dispatch the first job → the segment parks on INPUT_REQUIRED. */
  async function park(r: ReturnType<typeof makeHitlRuntime>) {
    const ran = await r.chat.run({ prompt: 'hi' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);
    return ran.threadId;
  }

  it('parks as a durable state transition — no tool run, no blocking process, no lock', async () => {
    const r = makeHitlRuntime();
    const threadId = await park(r);

    expect(r.sent).toEqual([]);
    expect(states(r.bus)).toEqual(['RUNNING', 'WAITING_FOR_INPUT']);
    expect(await r.kv.get(`agent:state:${threadId}`)).toBe('WAITING_FOR_INPUT');
    // The segment ended: the run lock is released while parked
    expect(await r.kv.get(`agent:lock:${threadId}`)).toBeNull();

    // Persisted history holds the tool CALL but never the park sentinel
    expect(roles(r.storage, threadId)).toEqual(['user', 'assistant']);

    // INPUT_REQUIRED carries the resume ticket for the queue dispatch
    const req = r.storage.events.store.get(threadId)!.find((e) => e.type === 'INPUT_REQUIRED')!;
    expect(req.payload).toMatchObject({
      toolCallId: 'call_1',
      toolName: 'sendEmail',
      arguments: { to: 'a@b.c' },
      resume: { agent: 'chat', model: 'gpt-4o' },
    });

    // §4: the steps up to the park are billed
    expect(stripThread(r.storage.usage.recorded)).toEqual([
      { agentId: 'chat', inputTokens: 10, cachedInputTokens: 0, outputTokens: 5, totalTokens: 15 },
    ]);
  });

  it('a redelivered job while parked is a no-op — the thread stays parked', async () => {
    const r = makeHitlRuntime();
    const threadId = await park(r);
    const before = r.storage.messages.store.get(threadId)!.length;

    await r.runtime.worker.handleJob({ threadId, model: 'gpt-4o', agent: 'chat' });

    expect(await r.kv.get(`agent:state:${threadId}`)).toBe('WAITING_FOR_INPUT');
    expect(r.storage.messages.store.get(threadId)!.length).toBe(before);
    expect(r.sent).toEqual([]);
  });

  it('respond(approved) resumes via the queue: the real tool runs and the loop continues', async () => {
    const r = makeHitlRuntime();
    const threadId = await park(r);

    const res = await r.runtime.hitl.respond({ threadId, toolCallId: 'call_1', approved: true });
    expect(res.delivered).toBe(true);

    // The resume job is the SAME dispatch path, rebuilt from the ticket
    const resumeJob = r.queue.items.at(-1)!;
    expect(resumeJob).toMatchObject({ threadId, model: 'gpt-4o', agent: 'chat' });

    await r.runtime.worker.handleJob(resumeJob);

    expect(r.sent).toEqual([1]); // the real tool ran in the resumed segment
    expect(states(r.bus).slice(-2)).toEqual(['RUNNING', 'COMPLETED']);
    expect(roles(r.storage, threadId)).toEqual([
      'user', 'assistant', 'tool', 'assistant',
    ]);
    const verdict = r.storage.messages.store.get(threadId)![2]!;
    expect((verdict.content as any)[0].result).toEqual({ status: 'SENT' });
    expect(lastTerminal(r.bus).payload).toMatchObject({ state: 'COMPLETED', stopReason: 'completed' });
  });

  it('respond(denied) appends the denial — the tool never runs', async () => {
    const r = makeHitlRuntime();
    const threadId = await park(r);

    await r.runtime.hitl.respond({ threadId, toolCallId: 'call_1', approved: false });
    await r.runtime.worker.handleJob(r.queue.items.at(-1)!);

    expect(r.sent).toEqual([]);
    const verdict = r.storage.messages.store.get(threadId)![2]!;
    expect((verdict.content as any)[0].result).toEqual({ denied: true });
    expect(lastTerminal(r.bus).payload).toMatchObject({ state: 'COMPLETED', stopReason: 'completed' });
  });

  it('TTL expiry becomes the timeout denial and the run continues (§2.5)', async () => {
    const r = makeHitlRuntime({ hitlTtlMs: 30 });
    const threadId = await park(r);

    // Backdate the INPUT_REQUIRED past the TTL — the answer key never existed
    const events = r.storage.events.store.get(threadId)!;
    const req = events.find((e) => e.type === 'INPUT_REQUIRED')!;
    (req as any).createdAt = new Date(Date.now() - 60_000);

    await r.runtime.worker.handleJob({ threadId, model: 'gpt-4o', agent: 'chat' });

    expect(r.sent).toEqual([]); // no answer → never executed
    expect(r.bus.published.some((e) => e.type === 'INPUT_EXPIRED')).toBe(true);
    const verdict = r.storage.messages.store.get(threadId)![2]!;
    expect((verdict.content as any)[0].result).toEqual({
      responded: false, cancelled: true, reason: 'timeout',
    });
    // The model still gets its turn: the loop continued to the final step
    expect(lastTerminal(r.bus).payload).toMatchObject({ state: 'COMPLETED', stopReason: 'completed' });
  });

  it('stop() while parked wins: the thread stays CANCELLED and respond is rejected', async () => {
    const r = makeHitlRuntime();
    const threadId = await park(r);

    const stopped = await r.chat.stop(threadId);
    expect(stopped.accepted).toBe(true);

    const res = await r.runtime.hitl.respond({ threadId, toolCallId: 'call_1', approved: true });
    expect(res.delivered).toBe(false); // not WAITING_FOR_INPUT anymore

    // A late resume dispatch must not resurrect the stopped thread
    await r.runtime.worker.handleJob({ threadId, model: 'gpt-4o', agent: 'chat' });
    expect(await r.kv.get(`agent:state:${threadId}`)).toBe('CANCELLED');
    expect(r.sent).toEqual([]);
  });

  // The park used to have no deadline of its own: hitlTtlMs was only ever read
  // when something else woke the thread, so an approval nobody watched never
  // expired at all.
  it('the park schedules its own expiry, timed just past the TTL', async () => {
    const r = makeHitlRuntime({ hitlTtlMs: 30_000, reclaimGraceMs: 5_000 });
    const ran = await r.chat.run({ prompt: 'hi' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    expect(r.queue.items.at(-1)).toMatchObject({
      threadId: ran.threadId,
      runId: ran.runId,   // the PARKED run, not a new one
      model: 'gpt-4o',
      agent: 'chat',
    });
    expect(r.queue.delays.at(-1)).toBe(35); // (hitlTtlMs + reclaimGraceMs) / 1000
  });

  it('the scheduled expiry resolves an abandoned park with nobody watching', async () => {
    const r = makeHitlRuntime({ hitlTtlMs: 30 });
    const threadId = await park(r);
    const timer = r.queue.items.at(-1)!;

    // The TTL passes with no client connected and no reclaim call.
    const req = r.storage.events.store
      .get(threadId)!
      .find((e) => e.type === 'INPUT_REQUIRED')!;
    (req as any).createdAt = new Date(Date.now() - 60_000);

    await r.runtime.worker.handleJob(timer);

    expect(r.sent).toEqual([]); // never answered → never executed
    expect((r.storage.messages.store.get(threadId)![2]!.content as any)[0].result).toEqual({
      responded: false, cancelled: true, reason: 'timeout',
    });
    expect(lastTerminal(r.bus).payload).toMatchObject({ state: 'COMPLETED' });
  });

  it('an expiry job delivered early leaves the thread parked', async () => {
    // A queue adapter that ignores the delay must not cut the approval short.
    const r = makeHitlRuntime({ hitlTtlMs: 60_000 });
    const threadId = await park(r);
    const before = roles(r.storage, threadId);

    await r.runtime.worker.handleJob(r.queue.items.at(-1)!);

    expect(roles(r.storage, threadId)).toEqual(before);
    expect(await r.kv.get(`agent:state:${threadId}`)).toBe('WAITING_FOR_INPUT');
  });

  // The invariant that keeps the answer and the expiry from becoming rival
  // runs. Mint a fresh id on respond and they can both run a segment — the
  // second one replying to a conversation that already ended.
  it('respond reuses the parked run id: the answer and the expiry are ONE run', async () => {
    const r = makeHitlRuntime({ hitlTtlMs: 30 });
    const ran = await r.chat.run({ prompt: 'hi' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);
    const timer = r.queue.items.at(-1)!;

    await r.runtime.hitl.respond({ threadId: ran.threadId, toolCallId: 'call_1', approved: true });
    const answer = r.queue.items.at(-1)!;

    expect(answer.runId).toBe(ran.runId!);
    expect(answer.runId).toBe(timer.runId!);

    await r.runtime.worker.handleJob(answer);
    const settled = roles(r.storage, ran.threadId);

    // The expiry lands late on a finished run and changes nothing.
    await r.runtime.worker.handleJob(timer);

    expect(r.sent).toEqual([1]); // the tool ran once
    expect(roles(r.storage, ran.threadId)).toEqual(settled); // no second reply
    expect(await r.kv.get(`agent:state:${ran.threadId}`)).toBe('COMPLETED');
  });

  it('an expiry job that wins the race to an answered park honours the answer', async () => {
    const r = makeHitlRuntime({ hitlTtlMs: 30 });
    const threadId = await park(r);
    const timer = r.queue.items.at(-1)!;

    // The verdict is recorded, but the expiry job reaches the lock first.
    await r.kv.set('agent:hitl:call_1', JSON.stringify({ approved: true }));
    await r.runtime.worker.handleJob(timer);

    expect(r.sent).toEqual([1]); // approved, not denied by timeout
    // It still owned the run, so it wrote the state rather than going silent.
    expect(await r.kv.get(`agent:state:${threadId}`)).toBe('COMPLETED');
  });
});
