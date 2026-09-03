import { describe, expect, it } from 'bun:test';
import { tool, simulateReadableStream } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { z } from 'zod';
import { MockLanguageModelV1 } from 'ai/test';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { bindStorage } from '../src/core/state.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { markRequiresConfirmation } from '../src/core/engine.js';
import { parkForInput } from '../src/core/hitl.js';
import { resolveConfig, type AgentConfig } from '../src/core/types.js';
import type { RuntimeOptions } from '../src/ports/runtime.js';

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

async function makeRuntime(model: any, config: Partial<AgentConfig> = {}) {
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
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
    config: resolveConfig(config),
  };
  return { deps, store: bindStorage(storage, { state: {} }), runtime: await setupAgentCore(deps), storage, bus, queue, kv };
}

const states = (bus: MemoryBus) =>
  bus.published.filter((e) => e.type === 'STATE_CHANGE').map((e) => (e.payload as any).state);
const lastTerminal = (bus: MemoryBus) =>
  bus.published.filter((e) => e.type === 'STATE_CHANGE').at(-1)!;
const roles = (storage: MemoryStorage, threadId: string) =>
  storage.messages.store.get(threadId)!.map((m) => m.role);

describe('reconnecting mid-run (§2.2)', () => {
  const assistantText = (snap: any) =>
    snap.messages
      .filter((m: any) => m.role === 'assistant')
      .map((m: any) =>
        (Array.isArray(m.content) ? m.content : []).map((p: any) => p?.text ?? '').join(''),
      )
      .join('');
  const replayedText = (snap: any) =>
    snap.activeEvents
      .filter((e: any) => e.type === 'CHUNK' && e.payload?.type === 'text-delta')
      .map((e: any) => e.payload.textDelta)
      .join('');

  // A client rebuilds from durable messages and THEN replays activeEvents. So
  // a step whose messages are already committed must not have its chunks
  // replayed as well, or its text lands twice — once from the message, once
  // from the stream that produced it.
  it('replays only the step that has not been committed yet', async () => {
    let snap: any = null;
    let r: any;
    let threadId: string | undefined;

    r = await makeRuntime(
      scriptedModel([
        { text: 'PART ONE. ', toolCalls: [{ toolCallId: 'c1', toolName: 'probe', args: { n: 1 } }] },
        { text: 'PART TWO. ', toolCalls: [{ toolCallId: 'c2', toolName: 'probe', args: { n: 2 } }] },
        { text: 'DONE.' },
      ]),
    );
    const chat = r.runtime.createStreamTextAgent({
      name: 'chat',
      model: 'gpt-4o',
      tools: {
        // Runs during step 2 — after step 1's messages are durable.
        probe: tool({
          parameters: z.object({ n: z.number() }),
          execute: async ({ n }: any) => {
            if (n === 2) snap = await r.runtime.getThreadSnapshot(threadId!);
            return { ok: true };
          },
        }),
      },
    });

    const ran = await chat.run({ prompt: 'go' });
    threadId = ran.threadId;
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    expect(snap).not.toBeNull();
    // Step 1 is durable, step 2 is still in flight — each appears exactly once
    expect(assistantText(snap)).toBe('PART ONE. ');
    expect(replayedText(snap)).toBe('PART TWO. ');
    expect(assistantText(snap) + replayedText(snap)).toBe('PART ONE. PART TWO. ');
  });

  // Nothing is committed during the very first step, so its chunks are the
  // only record of it and must all replay.
  it('replays everything when no step has committed', async () => {
    let snap: any = null;
    let r: any;
    let threadId: string | undefined;

    r = await makeRuntime(
      scriptedModel([
        { text: 'ONLY. ', toolCalls: [{ toolCallId: 'c1', toolName: 'probe', args: { n: 1 } }] },
        { text: 'DONE.' },
      ]),
    );
    const chat = r.runtime.createStreamTextAgent({
      name: 'chat',
      model: 'gpt-4o',
      tools: {
        probe: tool({
          parameters: z.object({ n: z.number() }),
          execute: async ({ n }: any) => {
            if (n === 1) snap = await r.runtime.getThreadSnapshot(threadId!);
            return { ok: true };
          },
        }),
      },
    });

    const ran = await chat.run({ prompt: 'go' });
    threadId = ran.threadId;
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    expect(assistantText(snap)).toBe('');
    expect(replayedText(snap)).toBe('ONLY. ');
  });

  // A park is published DURING the step, before its messages commit. Slicing
  // the whole window at the commit boundary would drop the very approval the
  // reconnecting client needs to render.
  it('keeps a pending approval that was raised before the step committed', async () => {
    const { runtime, queue } = await makeRuntime(
      scriptedModel([
        { text: 'about to send. ', toolCalls: [{ toolCallId: 'c1', toolName: 'sendEmail', args: { to: 'a@b.com' } }] },
        { text: 'sent.' },
      ]),
    );
    const chat = runtime.createStreamTextAgent({
      name: 'chat',
      model: 'gpt-4o',
      tools: {
        sendEmail: markRequiresConfirmation(
          tool({
            parameters: z.object({ to: z.string() }),
            execute: async ({ to }: any) => ({ sent: to }),
          }),
        ),
      },
    });

    const ran = await chat.run({ prompt: 'send it' });
    await runtime.worker.handleJob(queue.items[0]!);

    const snap = (await runtime.getThreadSnapshot(ran.threadId))!;
    expect(snap.thread.state).toBe('WAITING_FOR_INPUT');
    expect(snap.activeEvents.map((e: any) => e.type)).toContain('INPUT_REQUIRED');
  });
});

describe('providerOptions (§3.1)', () => {
  /** Captures what the provider is actually handed. Asserting on what we
   *  passed in proves only that an object was copied. */
  function capturing(seen: { options?: any }) {
    return new MockLanguageModelV1({
      provider: 'mock',
      modelId: 'mock-po',
      doStream: async (call: any) => {
        seen.options = call.providerMetadata ?? call.providerOptions;
        return {
          stream: simulateReadableStream({
            chunks: [
              { type: 'text-delta', textDelta: 'ok' },
              { type: 'finish', finishReason: 'stop', usage: { promptTokens: 5, completionTokens: 1 } },
            ] as LanguageModelV1StreamPart[],
          }),
          rawCall: { rawPrompt: null, rawSettings: {} },
        };
      },
    });
  }

  async function runWith(
    config: Partial<AgentConfig>,
    spec?: Record<string, unknown>,
    runOpts?: Record<string, unknown>,
  ) {
    const seen: { options?: any } = {};
    const { runtime, queue, store } = await makeRuntime(capturing(seen), config);
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o', ...spec } as any);
    const threadId = (await store.threads.create({ model: 'gpt-4o' })).id;
    await chat.run({ threadId, prompt: 'hi', ...runOpts } as any);
    await runtime.worker.handleJob(queue.items.at(-1)!);
    return seen.options;
  }

  // The widest level: set once at setupAgentCore, applied to every run.
  it('applies the runtime-wide default', async () => {
    const options = await runWith({ providerOptions: { openai: { serviceTier: 'flex' } } });
    expect(options).toMatchObject({ openai: { serviceTier: 'flex' } });
  });

  it('lets an agent spec override the runtime default', async () => {
    const options = await runWith(
      { providerOptions: { openai: { serviceTier: 'flex' } } },
      { providerOptions: { openai: { serviceTier: 'priority' } } },
    );
    expect(options).toMatchObject({ openai: { serviceTier: 'priority' } });
  });

  it('lets a run override both', async () => {
    const options = await runWith(
      { providerOptions: { openai: { serviceTier: 'flex' } } },
      { providerOptions: { openai: { serviceTier: 'priority' } } },
      { providerOptions: { openai: { serviceTier: 'auto' } } },
    );
    expect(options).toMatchObject({ openai: { serviceTier: 'auto' } });
  });

  // Merging is per provider namespace, so options for a provider nobody
  // overrode survive.
  it('keeps a namespace no later level mentions', async () => {
    const options = await runWith(
      { providerOptions: { anthropic: { thinking: { type: 'enabled' } } } },
      { providerOptions: { openai: { serviceTier: 'flex' } } },
    );
    expect(options).toMatchObject({
      anthropic: { thinking: { type: 'enabled' } },
      openai: { serviceTier: 'flex' },
    });
  });

  it('sends nothing when no level sets it', async () => {
    expect(await runWith({})).toBeUndefined();
  });
});

describe('engine loop (§2.1, §5.6): platform-owned continuation', () => {
  it('feeds tool results back between single-round-trip steps and persists per step', async () => {
    const executed: string[] = [];
    const { runtime, storage, bus, queue } = await makeRuntime(
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

    // §4 attribution: ONE row per model call, not one per segment. The main
    // run's rows carry no agentId; the name that bills is agentName.
    const rows = storage.usage.recorded;
    expect(rows.length).toBe(2);
    rows.forEach((r, i) => {
      expect(r).toMatchObject({
        agentId: null,
        agentName: 'chat',
        runId: ran.runId,
        kind: 'step',
        step: i + 1,
        outcome: 'finished',
        model: 'gpt-4o',
        inputTokens: 10,
        outputTokens: 5,
        totalTokens: 15,
      });
      // Nothing priced this runtime, so the row is stored without a cost.
      expect(r.cost).toBeFalsy();
    });
  });

  it('checks the budget BETWEEN steps: the step that crosses the line is kept in full', async () => {
    const executed: string[] = [];
    const { runtime, storage, bus, queue } = await makeRuntime(
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
    // The break was announced before the run ended
    const exhausted = bus.published.find((e) => e.type === 'TOKEN_BUDGET_EXHAUSTED')!;
    expect(exhausted.payload).toEqual({ agentId: null, tokensUsed: 240, tokenBudget: 150 });
    expect(exhausted.seq).toBeLessThan(lastTerminal(bus).seq);
    expect(roles(storage, ran.threadId)).toEqual(['user', 'assistant', 'tool', 'assistant']);
    const last = storage.messages.store.get(ran.threadId)!.at(-1)!;
    expect((last.content as any)[0].text).toBe('done');
  });

  it('finalizes max_steps when the ceiling is hit with pending tool calls', async () => {
    const { runtime, storage, bus, queue } = await makeRuntime(
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
    const { runtime, bus, queue } = await makeRuntime(scriptedModel([{ text: 'answer' }]));
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
  async function makeHitlRuntime(config: Partial<AgentConfig> = {}) {
    const sent: number[] = [];
    const { runtime, storage, bus, queue, kv } = await makeRuntime(
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
  async function park(r: Awaited<ReturnType<typeof makeHitlRuntime>>) {
    const ran = await r.chat.run({ prompt: 'hi' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);
    return ran.threadId;
  }

  it('parks as a durable state transition — no tool run, no blocking process, no lock', async () => {
    const r = await makeHitlRuntime();
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

    // §4: the calls up to the park are billed, one row each
    expect(r.storage.usage.recorded.length).toBe(1);
    expect(r.storage.usage.recorded[0]).toMatchObject({
      agentId: null, agentName: 'chat', kind: 'step', step: 1,
      inputTokens: 10, outputTokens: 5, totalTokens: 15,
    });
  });

  it('a redelivered job while parked is a no-op — the thread stays parked', async () => {
    const r = await makeHitlRuntime();
    const threadId = await park(r);
    const before = r.storage.messages.store.get(threadId)!.length;

    await r.runtime.worker.handleJob({ threadId, model: 'gpt-4o', agent: 'chat' });

    expect(await r.kv.get(`agent:state:${threadId}`)).toBe('WAITING_FOR_INPUT');
    expect(r.storage.messages.store.get(threadId)!.length).toBe(before);
    expect(r.sent).toEqual([]);
  });

  it('respond(approved) resumes via the queue: the real tool runs and the loop continues', async () => {
    const r = await makeHitlRuntime();
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
    const r = await makeHitlRuntime();
    const threadId = await park(r);

    await r.runtime.hitl.respond({ threadId, toolCallId: 'call_1', approved: false });
    await r.runtime.worker.handleJob(r.queue.items.at(-1)!);

    expect(r.sent).toEqual([]);
    const verdict = r.storage.messages.store.get(threadId)![2]!;
    expect((verdict.content as any)[0].result).toEqual({ denied: true });
    expect(lastTerminal(r.bus).payload).toMatchObject({ state: 'COMPLETED', stopReason: 'completed' });
  });

  it('TTL expiry becomes the timeout denial and the run continues (§2.5)', async () => {
    const r = await makeHitlRuntime({ hitlTtlMs: 30 });
    const threadId = await park(r);

    // Backdate the INPUT_REQUIRED past the TTL — the answer key never existed
    const events = r.storage.events.store.get(threadId)!;
    const req = events.find((e) => e.type === 'INPUT_REQUIRED')!;
    (req as any).createdAt = new Date(Date.now() - 60_000);
    (req.payload as any).expiresAt = new Date(Date.now() - 60_000).toISOString(); // the park's own deadline, too

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
    const r = await makeHitlRuntime();
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
    const r = await makeHitlRuntime({ hitlTtlMs: 30_000, reclaimGraceMs: 5_000 });
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
    const r = await makeHitlRuntime({ hitlTtlMs: 30 });
    const threadId = await park(r);
    const timer = r.queue.items.at(-1)!;

    // The TTL passes with no client connected and no reclaim call.
    const req = r.storage.events.store
      .get(threadId)!
      .find((e) => e.type === 'INPUT_REQUIRED')!;
    (req as any).createdAt = new Date(Date.now() - 60_000);
    (req.payload as any).expiresAt = new Date(Date.now() - 60_000).toISOString(); // the park's own deadline, too

    await r.runtime.worker.handleJob(timer);

    expect(r.sent).toEqual([]); // never answered → never executed
    expect((r.storage.messages.store.get(threadId)![2]!.content as any)[0].result).toEqual({
      responded: false, cancelled: true, reason: 'timeout',
    });
    expect(lastTerminal(r.bus).payload).toMatchObject({ state: 'COMPLETED' });
  });

  it('an expiry job delivered early leaves the thread parked', async () => {
    // A queue adapter that ignores the delay must not cut the approval short.
    const r = await makeHitlRuntime({ hitlTtlMs: 60_000 });
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
    const r = await makeHitlRuntime({ hitlTtlMs: 30 });
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
    const r = await makeHitlRuntime({ hitlTtlMs: 30 });
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

describe('a tool that parks itself (§2.5)', () => {
  // A tool that starts work it cannot wait for parks ITSELF: the run lock and
  // the worker are released, the request is durable with the tool's own
  // reason and deadline, and whoever finishes the work resumes the same call
  // through respond with a payload the tool then returns from.
  it('starts the work, parks, and resumes the same call with the payload', async () => {
    const started: string[] = [];
    const resumedWith: unknown[] = [];
    const r = await makeRuntime(
      scriptedModel([
        { toolCalls: [{ toolCallId: 'c1', toolName: 'render', args: { scene: 'intro' } }] },
        { text: 'rendered' },
      ]),
      { hitlTtlMs: 60 * 60_000 },
    );
    const chat = r.runtime.createStreamTextAgent({
      name: 'chat',
      model: 'gpt-4o',
      tools: {
        render: tool({
          description: 'renders',
          parameters: z.object({ scene: z.string() }),
          execute: async ({ scene }, opts: any) => {
            if (opts.approval) {
              resumedWith.push(opts.approval.payload);
              return { url: 'https://cdn/x.mp4' };
            }
            started.push(scene);
            throw parkForInput({ reason: 'job', payload: { jobId: 'job-1' }, ttlMs: 30 * 60_000 });
          },
        }),
      },
    });
    const ran = await chat.run({ prompt: 'render the intro' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    expect(started).toEqual(['intro']);
    expect(states(r.bus)).toEqual(['RUNNING', 'WAITING_FOR_INPUT']);
    expect(await r.kv.get(`agent:lock:${ran.threadId}`)).toBeNull();
    const req = r.storage.events.store.get(ran.threadId)!.find((e) => e.type === 'INPUT_REQUIRED')!;
    expect(req.payload).toMatchObject({ toolCallId: 'c1', toolName: 'render', reason: 'job', arguments: { jobId: 'job-1' } });
    const expiresIn = new Date((req.payload as any).expiresAt).getTime() - Date.now();
    expect(expiresIn).toBeGreaterThan(25 * 60_000);
    expect(expiresIn).toBeLessThanOrEqual(30 * 60_000);
    // The expiry job is timed to the park's own TTL, not the config's.
    expect(r.queue.delays.at(-1)).toBe(30 * 60 + (r.deps.config?.reclaimGraceMs ?? 0) / 1000);

    // The job completes: its owner responds with the outcome.
    const res = await r.runtime.hitl.respond({
      threadId: ran.threadId, toolCallId: 'c1', approved: true, payload: { status: 'succeeded' },
    });
    expect(res.delivered).toBe(true);
    await r.runtime.worker.handleJob(r.queue.items.at(-1)!);

    expect(resumedWith).toEqual([{ status: 'succeeded' }]);
    expect(lastTerminal(r.bus).payload).toMatchObject({ state: 'COMPLETED' });
    expect(roles(r.storage, ran.threadId)).toEqual(['user', 'assistant', 'tool', 'assistant']);
  });

  it('expires on its own deadline, not the config\'s', async () => {
    const r = await makeRuntime(
      scriptedModel([
        { toolCalls: [{ toolCallId: 'c1', toolName: 'render', args: {} }] },
        { text: 'gave up' },
      ]),
      { hitlTtlMs: 60 * 60_000, reclaimGraceMs: 0 },
    );
    const chat = r.runtime.createStreamTextAgent({
      name: 'chat',
      model: 'gpt-4o',
      tools: {
        render: tool({
          description: 'renders',
          parameters: z.object({}),
          execute: async (_args, opts: any): Promise<unknown> => {
            if (opts.approval) throw new Error('an expired park must not run the tool again');
            throw parkForInput({ reason: 'job', ttlMs: 20 });
          },
        }),
      },
    });
    const ran = await chat.run({ prompt: 'render' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);
    expect(states(r.bus).at(-1)).toBe('WAITING_FOR_INPUT');

    await new Promise((resolve) => setTimeout(resolve, 30));
    await r.runtime.worker.handleJob(r.queue.items.at(-1)!); // the park's own expiry job
    expect(lastTerminal(r.bus).payload).toMatchObject({ state: 'COMPLETED' });
    expect(r.bus.published.some((e) => e.type === 'INPUT_EXPIRED')).toBe(true);
  });
});
