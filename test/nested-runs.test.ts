import { describe, expect, it } from 'bun:test';
import { simulateReadableStream, tool } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { z } from 'zod';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { markRequiresConfirmation } from '../src/core/engine.js';
import { compactContext } from '../src/core/context.js';
import { resolveConfig, type AgentConfig } from '../src/core/types.js';
import type { RuntimePorts } from '../src/ports/runtime.js';

const finish = (reason: string, usage = { promptTokens: 10, completionTokens: 5 }) =>
  ({ type: 'finish', finishReason: reason, usage }) as LanguageModelV1StreamPart;
const call = (id: string, name: string, args: unknown) =>
  ({
    type: 'tool-call', toolCallType: 'function', toolCallId: id, toolName: name,
    args: JSON.stringify(args),
  }) as LanguageModelV1StreamPart;
const say = (text: string) => ({ type: 'text-delta', textDelta: text }) as LanguageModelV1StreamPart;

const stream = (chunks: LanguageModelV1StreamPart[]) => ({
  stream: simulateReadableStream({ chunks }),
  rawCall: { rawPrompt: null, rawSettings: {} },
});

/** A subagent is told who it is through `system`; that is how these scripts
 *  tell a nested run's round-trip apart from the parent's. */
const isChild = (prompt: any) =>
  (prompt ?? []).some(
    (m: any) =>
      m.role === 'system' &&
      String(typeof m.content === 'string' ? m.content : JSON.stringify(m.content)).includes(
        'subagent',
      ),
  );
const toolResults = (prompt: any) =>
  (prompt ?? []).flatMap((m: any) =>
    m.role === 'tool' ? (Array.isArray(m.content) ? m.content : []) : [],
  );

function makeRuntime(model: any, config: Partial<AgentConfig> = {}) {
  const storage = new MemoryStorage();
  const bus = new MemoryBus();
  const queue = new MemoryQueue();
  const kv = new MemoryKv();
  const deps: RuntimePorts = {
    storage, bus, queue, kv,
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
    config: resolveConfig(config),
  };
  return { deps, runtime: setupAgentCore(deps), storage, bus, queue, kv };
}

/** Parent delegates once; the child calls a destructive tool and parks. */
function delegatingModel(state: { childCalls: number; parentCalls: number }) {
  return new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock-delegating',
    doStream: async ({ prompt }: any) => {
      if (isChild(prompt)) {
        state.childCalls += 1;
        // Second time through, the approved tool result is in its context.
        return toolResults(prompt).some((r: any) => r.toolName === 'sendEmail')
          ? stream([say('mail is away'), finish('stop')])
          : stream([call('child_call_1', 'sendEmail', { to: 'a@b.c' }), finish('tool-calls')]);
      }
      state.parentCalls += 1;
      return toolResults(prompt).some((r: any) => r.toolName === 'spawnSubagent')
        ? stream([say('all set'), finish('stop')])
        : stream([
            call('parent_call_1', 'spawnSubagent', { name: 'mailer', instructions: 'send it' }),
            finish('tool-calls'),
          ]);
    },
  });
}

function delegatingRuntime(config: Partial<AgentConfig> = {}) {
  const state = { childCalls: 0, parentCalls: 0 };
  const sent: Array<{ to: string }> = [];
  const r = makeRuntime(delegatingModel(state), config);
  const chat = r.runtime.createStreamTextAgent({
    name: 'chat',
    model: 'gpt-4o',
    subagents: {
      // §2.7: extra tools are merged into every child and HITL-wrapped
      tools: {
        sendEmail: markRequiresConfirmation(
          tool({
            parameters: z.object({ to: z.string() }),
            execute: async ({ to }) => { sent.push({ to }); return { status: 'SENT', to }; },
          }),
        ),
      },
    },
  });
  return { ...r, chat, state, sent };
}

const streams = (storage: MemoryStorage, threadId: string) =>
  storage.messages.store.get(threadId)!.map((m) => `${m.agentId ?? 'main'}/${m.role}`);

describe('a nested run parks (§2.7)', () => {
  it('suspends the whole thread and records the chain waiting on the answer', async () => {
    const r = delegatingRuntime();
    const ran = await r.chat.run({ prompt: 'send the mail' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    expect(r.sent).toEqual([]); // parked, never executed
    expect((await r.storage.threads.get(ran.threadId))!.state).toBe('WAITING_FOR_INPUT');

    const req = r.bus.published.find((e) => e.type === 'INPUT_REQUIRED')!.payload as any;
    expect(req.toolName).toBe('sendEmail');
    expect(req.agentId).not.toBeNull();          // the CHILD asked
    expect(req.nested).toMatchObject({ name: 'mailer', depth: 1 });
    // The parent's spawnSubagent call is what waits on the answer
    expect(req.frames).toEqual([{ agentId: null, toolCallId: 'parent_call_1' }]);
  });

  it("keeps the child's turns in its own stream, out of the parent's context", async () => {
    const r = delegatingRuntime();
    const ran = await r.chat.run({ prompt: 'send the mail' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    const rows = r.storage.messages.store.get(ran.threadId)!;
    const childId = rows.find((m) => m.agentId !== null)!.agentId!;
    // main: the user turn + the assistant's spawnSubagent call (no result yet)
    // child: its brief + the assistant's sendEmail call
    expect(streams(r.storage, ran.threadId)).toEqual([
      'main/user', `${childId}/user`, `${childId}/assistant`, 'main/assistant',
    ]);

    // §2.6 compaction must never see the delegated turns
    const parentContext = await compactContext(r.deps, ran.threadId, 'gpt-4o');
    expect(parentContext.every((m) => m.agentId === null)).toBe(true);
    expect(parentContext).toHaveLength(2);
  });
});

describe('approving a nested park (§2.7)', () => {
  it('re-enters the child where it stopped and unwinds to the parent', async () => {
    const r = delegatingRuntime();
    const ran = await r.chat.run({ prompt: 'send the mail' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);
    const childCallsAtPark = r.state.childCalls;

    const res = await r.runtime.hitl.respond({
      threadId: ran.threadId, toolCallId: 'child_call_1', approved: true,
    });
    expect(res.delivered).toBe(true);
    await r.runtime.worker.handleJob(r.queue.items.at(-1)!);

    expect(r.sent).toEqual([{ to: 'a@b.c' }]); // the approved tool really ran
    // Re-entered, not restarted: exactly one more child round-trip.
    expect(r.state.childCalls).toBe(childCallsAtPark + 1);

    const rows = r.storage.messages.store.get(ran.threadId)!;
    const childId = rows.find((m) => m.agentId !== null)!.agentId!;
    expect(streams(r.storage, ran.threadId)).toEqual([
      'main/user',
      `${childId}/user`,
      `${childId}/assistant`,
      'main/assistant',
      `${childId}/tool`,      // the approved verdict lands in the CHILD's stream
      `${childId}/assistant`, // the child finishes
      'main/tool',            // its result answers the parent's spawnSubagent call
      'main/assistant',       // the parent's final turn
    ]);

    // The parent's dangling call was answered with the capped child result
    const parentResult = rows.find((m) => m.agentId === null && m.role === 'tool')!;
    expect((parentResult.content as any)[0]).toMatchObject({
      toolCallId: 'parent_call_1',
      toolName: 'spawnSubagent',
      result: { agentId: childId, result: 'mail is away' },
    });
    expect((await r.storage.threads.get(ran.threadId))!.state).toBe('COMPLETED');
  });

  it('a denial unwinds the same way, without running the tool', async () => {
    const r = delegatingRuntime();
    const ran = await r.chat.run({ prompt: 'send the mail' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    await r.runtime.hitl.respond({
      threadId: ran.threadId, toolCallId: 'child_call_1', approved: false,
    });
    await r.runtime.worker.handleJob(r.queue.items.at(-1)!);

    expect(r.sent).toEqual([]);
    const rows = r.storage.messages.store.get(ran.threadId)!;
    const verdict = rows.find((m) => m.agentId !== null && m.role === 'tool')!;
    expect((verdict.content as any)[0].result).toEqual({ denied: true });
    expect((await r.storage.threads.get(ran.threadId))!.state).toBe('COMPLETED');
  });

  it('an unanswered park still times out into the denial and unwinds', async () => {
    const r = delegatingRuntime({ hitlTtlMs: 30 });
    const ran = await r.chat.run({ prompt: 'send the mail' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    const req = r.storage.events.store
      .get(ran.threadId)!
      .find((e) => e.type === 'INPUT_REQUIRED')!;
    (req as any).createdAt = new Date(Date.now() - 60_000);

    await r.runtime.worker.handleJob(r.queue.items.at(-1)!);

    expect(r.sent).toEqual([]);
    const rows = r.storage.messages.store.get(ran.threadId)!;
    const verdict = rows.find((m) => m.agentId !== null && m.role === 'tool')!;
    expect((verdict.content as any)[0].result).toEqual({
      responded: false, cancelled: true, reason: 'timeout',
    });
    expect((await r.storage.threads.get(ran.threadId))!.state).toBe('COMPLETED');
  });

  it('a redelivery while still unanswered leaves the thread parked', async () => {
    const r = delegatingRuntime();
    const ran = await r.chat.run({ prompt: 'send the mail' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);
    const before = streams(r.storage, ran.threadId);

    await r.runtime.worker.handleJob({ threadId: ran.threadId, model: 'gpt-4o', agent: 'chat' });

    expect(streams(r.storage, ran.threadId)).toEqual(before);
    expect(await r.kv.get(`agent:state:${ran.threadId}`)).toBe('WAITING_FOR_INPUT');
  });
});

/** Parent delegates once, the child answers, nobody parks. */
function plainDelegatingRuntime(config: Partial<AgentConfig> = {}) {
  const state = { childCalls: 0, parentCalls: 0 };
  const model = new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock-plain',
    doStream: async ({ prompt }: any) => {
      if (isChild(prompt)) {
        state.childCalls += 1;
        return stream([say('did it'), finish('stop')]);
      }
      state.parentCalls += 1;
      return toolResults(prompt).some((r: any) => r.toolName === 'spawnSubagent')
        ? stream([say('all set'), finish('stop')])
        : stream([
            call('parent_call_1', 'spawnSubagent', { name: 'helper', instructions: 'do it' }),
            finish('tool-calls'),
          ]);
    },
  });
  const r = makeRuntime(model, config);
  const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o', subagents: true });
  return { ...r, chat, state };
}

const terminal = (bus: MemoryBus) =>
  bus.published.filter((e) => e.type === 'STATE_CHANGE').at(-1)!.payload as any;

describe('the run-wide token ledger (§2.7)', () => {
  it("counts a child's spend into the run's total, and still attributes it", async () => {
    const r = plainDelegatingRuntime();
    const ran = await r.chat.run({ prompt: 'delegate' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    // Two parent steps and one child step, 15 tokens each.
    expect(r.state.parentCalls).toBe(2);
    expect(r.state.childCalls).toBe(1);
    expect(terminal(r.bus).tokensUsed).toBe(45);

    // Still attributed per agent for billing (§4)
    const rows = r.storage.usage.recorded.filter((u) => u.threadId === ran.threadId);
    expect(rows.some((u) => u.agentId === 'chat')).toBe(true);
    expect(rows.some((u) => u.agentId !== 'chat' && u.totalTokens === 15)).toBe(true);
  });

  it("stops the run when the CHILD's spend crosses the budget", async () => {
    // The parent's first step alone spends 15; the child inside it spends
    // another 15. A cap of 20 is only reached because the child counts.
    const r = plainDelegatingRuntime({ tokenBudget: 20 });
    await r.chat.run({ prompt: 'delegate' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    expect(r.state.parentCalls).toBe(1); // the second parent step never started
    expect(terminal(r.bus)).toMatchObject({ state: 'COMPLETED', stopReason: 'token_budget' });
  });
});

/** Parent delegates to TWO children in one step; both park. */
function twoSiblingsRuntime(config: Partial<AgentConfig> = {}) {
  const sent: string[] = [];
  const firstUserText = (prompt: any) => {
    const u = (prompt ?? []).find((m: any) => m.role === 'user');
    const c = u?.content;
    return typeof c === 'string' ? c : (c ?? []).map((p: any) => p?.text ?? '').join('');
  };
  const model = new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock-siblings',
    doStream: async ({ prompt }: any) => {
      if (isChild(prompt)) {
        const who = firstUserText(prompt); // 'alpha' | 'beta'
        return toolResults(prompt).some((t: any) => t.toolName === 'sendEmail')
          ? stream([say(`${who} done`), finish('stop')])
          : stream([call(`${who}_call`, 'sendEmail', { to: `${who}@x.c` }), finish('tool-calls')]);
      }
      const spawned = toolResults(prompt).filter((t: any) => t.toolName === 'spawnSubagent');
      return spawned.length === 2
        ? stream([say('both done'), finish('stop')])
        : stream([
            call('spawn_alpha', 'spawnSubagent', { name: 'alpha', instructions: 'alpha' }),
            call('spawn_beta', 'spawnSubagent', { name: 'beta', instructions: 'beta' }),
            finish('tool-calls'),
          ]);
    },
  });
  const r = makeRuntime(model, config);
  const chat = r.runtime.createStreamTextAgent({
    name: 'chat',
    model: 'gpt-4o',
    subagents: {
      tools: {
        sendEmail: markRequiresConfirmation(
          tool({
            parameters: z.object({ to: z.string() }),
            execute: async ({ to }) => { sent.push(to); return { status: 'SENT', to }; },
          }),
        ),
      },
    },
  });
  return { ...r, chat, sent };
}

describe('two siblings park in one step (§2.7)', () => {
  it('waits for BOTH approvals, then unwinds both chains', async () => {
    const r = twoSiblingsRuntime();
    const ran = await r.chat.run({ prompt: 'send both' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    const requests = r.bus.published.filter((e) => e.type === 'INPUT_REQUIRED');
    expect(requests).toHaveLength(2);
    expect(requests.map((e) => (e.payload as any).frames[0].toolCallId).sort())
      .toEqual(['spawn_alpha', 'spawn_beta']);
    // Each chain waits on its OWN spawnSubagent call, not a shared one.
    expect(new Set(requests.map((e) => (e.payload as any).agentId)).size).toBe(2);

    // Answering one is not enough: the thread stays parked.
    await r.runtime.hitl.respond({
      threadId: ran.threadId, toolCallId: 'alpha_call', approved: true,
    });
    await r.runtime.worker.handleJob(r.queue.items.at(-1)!);
    expect(r.sent).toEqual([]);
    expect(await r.kv.get(`agent:state:${ran.threadId}`)).toBe('WAITING_FOR_INPUT');

    // The second answer releases the whole run.
    await r.runtime.hitl.respond({
      threadId: ran.threadId, toolCallId: 'beta_call', approved: true,
    });
    await r.runtime.worker.handleJob(r.queue.items.at(-1)!);

    expect(r.sent.sort()).toEqual(['alpha@x.c', 'beta@x.c']);
    const rows = r.storage.messages.store.get(ran.threadId)!;
    const parentResults = rows
      .filter((m) => m.agentId === null && m.role === 'tool')
      .flatMap((m) => (m.content as any[]).map((p) => p.toolCallId));
    expect(parentResults.sort()).toEqual(['spawn_alpha', 'spawn_beta']);
    expect((await r.storage.threads.get(ran.threadId))!.state).toBe('COMPLETED');
  });

  it('a mixed verdict still releases the run: one approved, one denied', async () => {
    const r = twoSiblingsRuntime();
    const ran = await r.chat.run({ prompt: 'send both' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    await r.runtime.hitl.respond({ threadId: ran.threadId, toolCallId: 'alpha_call', approved: true });
    await r.runtime.hitl.respond({ threadId: ran.threadId, toolCallId: 'beta_call', approved: false });
    await r.runtime.worker.handleJob(r.queue.items.at(-1)!);

    expect(r.sent).toEqual(['alpha@x.c']); // only the approved one ran
    expect((await r.storage.threads.get(ran.threadId))!.state).toBe('COMPLETED');
  });
});

describe('a model name the LLM invented (§2.7)', () => {
  it("falls back to the parent's model instead of killing the child", async () => {
    const state = { childCalls: 0, parentCalls: 0 };
    const model = new MockLanguageModelV1({
      provider: 'mock',
      modelId: 'mock-badmodel',
      doStream: async ({ prompt }: any) => {
        if (isChild(prompt)) {
          state.childCalls += 1;
          return stream([say('did it'), finish('stop')]);
        }
        state.parentCalls += 1;
        return toolResults(prompt).some((t: any) => t.toolName === 'spawnSubagent')
          ? stream([say('all set'), finish('stop')])
          : stream([
              call('parent_call_1', 'spawnSubagent', {
                name: 'helper', instructions: 'do it', model: 'gpt-3.5-invented',
              }),
              finish('tool-calls'),
            ]);
      },
    });
    const storage = new MemoryStorage();
    const bus = new MemoryBus();
    const queue = new MemoryQueue();
    const kv = new MemoryKv();
    const deps: RuntimePorts = {
      storage, bus, queue, kv,
      // A strict registry, like the reference app's (§3.3)
      resolveModel: (name) => {
        if (name !== 'gpt-4o') throw new Error(`Unknown model: ${name}`);
        return { instance: () => model, contextWindow: 128_000 };
      },
      config: resolveConfig(),
    };
    const runtime = setupAgentCore(deps);
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o', subagents: true });

    const ran = await chat.run({ prompt: 'delegate' });
    await runtime.worker.handleJob(queue.items[0]!);

    expect(state.childCalls).toBe(1); // it ran, rather than dying on resolution
    expect(bus.published.some((e) => e.type === 'SUBAGENT_FAILED')).toBe(false);
    expect((await storage.threads.get(ran.threadId))!.state).toBe('COMPLETED');
  });
});

describe('rebuilding the subagent panel after a reload (§2.7)', () => {
  it('the snapshot carries nested runs, which outlive their events', async () => {
    const r = plainDelegatingRuntime();
    const ran = await r.chat.run({ prompt: 'delegate' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    const snapshot = (await r.runtime.getThreadSnapshot(ran.threadId))!;
    expect(snapshot.runs).toHaveLength(1);
    expect(snapshot.runs[0]).toMatchObject({ name: 'helper', depth: 1, state: 'COMPLETED' });

    // On a finished thread there are no active events left to replay, so the
    // runs are the only place a client can read a child's name and state.
    expect(snapshot.activeEvents).toHaveLength(0);
    // And the child's turns are in the log, under its own agentId.
    expect(
      snapshot.messages.filter((m) => m.agentId === snapshot.runs[0]!.id).length,
    ).toBeGreaterThan(0);
  });
});
