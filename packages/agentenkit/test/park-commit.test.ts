import { describe, expect, it } from 'bun:test';
import { simulateReadableStream, tool } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { z } from 'zod';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { bindStorage } from '../src/core/state.js';
import { markRequiresConfirmation } from '../src/core/engine.js';
import { hitlDoneKey, hitlKey, loadOpenHitls } from '../src/core/hitl.js';
import { repairDanglingToolCalls } from '../src/core/messages.js';
import { runIdKey } from '../src/core/keys.js';
import { resolveConfig } from '../src/core/types.js';
import type { RuntimePorts } from '../src/ports/runtime.js';

// Workstream D: a park is written only once its step is saved, an answer
// survives until its result is saved, and a failure on the way back up never
// leaves a thread stuck. The same cases run in the Go package
// (park_commit_test.go).

const finish = (reason: string) =>
  ({ type: 'finish', finishReason: reason, usage: { promptTokens: 10, completionTokens: 5 } }) as LanguageModelV1StreamPart;
const call = (id: string, name: string, args: unknown) =>
  ({ type: 'tool-call', toolCallType: 'function', toolCallId: id, toolName: name, args: JSON.stringify(args) }) as LanguageModelV1StreamPart;
const say = (text: string) => ({ type: 'text-delta', textDelta: text }) as LanguageModelV1StreamPart;
const stream = (chunks: LanguageModelV1StreamPart[]) => ({
  stream: simulateReadableStream({ chunks }),
  rawCall: { rawPrompt: null, rawSettings: {} },
});

/** Plays back one scripted step per model call; records every prompt. */
function scripted(steps: Array<LanguageModelV1StreamPart[] | Error>) {
  const prompts: any[] = [];
  const model = new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock',
    doStream: async ({ prompt }: any) => {
      prompts.push(prompt);
      const s = steps[Math.min(prompts.length - 1, steps.length - 1)]!;
      if (s instanceof Error) throw s;
      return stream(s);
    },
  });
  return { model, prompts };
}

/** A main agent with one approval tool ("send") and one plain tool ("lookup").
 *  Nested runs get the same "wipe" approval tool the Go tests use. */
async function setup(
  steps: Array<LanguageModelV1StreamPart[] | Error>,
  opts: { failFirstAssistantSave?: boolean; subagents?: boolean } = {},
) {
  const { model, prompts } = scripted(steps);
  const storage = new MemoryStorage();
  if (opts.failFirstAssistantSave) {
    const append = storage.messages.append.bind(storage.messages);
    let failed = false;
    storage.messages.append = async (t: string, m: any) => {
      if (m.role === 'assistant' && !failed) {
        failed = true;
        throw new Error('storage down');
      }
      return append(t, m);
    };
  }
  const bus = new MemoryBus();
  const queue = new MemoryQueue();
  const kv = new MemoryKv();
  const admin = new MemoryAdminStore();
  const config = resolveConfig({ stopPollMs: 20, runRetryBackoffMs: 1 });
  const runtime = await setupAgentCore({
    storage, bus, queue, kv, admin, config,
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
  });
  const sent: string[] = [];
  const wiped: string[] = [];
  const chat = runtime.createStreamTextAgent({
    name: 'chat',
    model: 'gpt-4o',
    tools: {
      send: markRequiresConfirmation(
        tool({ parameters: z.object({}), execute: async () => { sent.push('sent'); return { sent: true }; } }),
      ),
      lookup: tool({
        parameters: z.object({ bad: z.boolean().optional() }),
        execute: async ({ bad }) => {
          if (bad) throw new Error('bad input');
          return { found: 42 };
        },
      }),
    },
    ...(opts.subagents
      ? {
          subagents: {
            tools: {
              wipe: markRequiresConfirmation(
                tool({
                  parameters: z.object({ target: z.string() }),
                  execute: async ({ target }) => { wiped.push(target); return { wiped: true }; },
                }),
              ),
            },
          },
        }
      : {}),
  });
  const ports: RuntimePorts = {
    storage: bindStorage(storage, { state: {} }), bus, queue, kv, admin, config,
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
  };
  const state = async (threadId: string) => (await storage.threads.get(threadId))!.state;
  // Everything the thread saw, in the order it was sent: record entries and
  // live notices alike.
  const events = (threadId: string, type: string) =>
    bus.published.filter((e) => e.threadId === threadId && e.type === type);
  const order = (e: unknown) => bus.published.indexOf(e as any);
  const rows = (threadId: string, agentId: string | null = null) =>
    storage.messages.store.get(threadId)!.filter((m) => m.agentId === agentId);
  const next = () => runtime.worker.handleJob(queue.items.shift()!);
  return { runtime, chat, storage, queue, kv, ports, prompts, sent, wiped, state, events, order, rows, next };
}

describe('a park waits for its step (§2.5)', () => {
  it('is written only after its step is saved', async () => {
    const r = await setup([[call('a1', 'send', {}), finish('tool-calls')]]);
    const ran = await r.chat.run({ prompt: 'go' });
    await r.next();
    const committed = r.order(r.events(ran.threadId, 'STEP_COMMITTED')[0]);
    const requested = r.order(r.events(ran.threadId, 'INPUT_REQUIRED')[0]);
    expect(committed).toBeGreaterThan(-1);
    expect(requested).toBeGreaterThan(committed);
    expect(await r.state(ran.threadId)).toBe('WAITING_FOR_INPUT');
    expect(r.rows(ran.threadId).map((m) => m.role)).toEqual(['user', 'assistant']);
  });

  it('a step that fails after a tool asked writes no park', async () => {
    const r = await setup(
      [[call('a1', 'send', {}), finish('tool-calls')], [say('ok'), finish('stop')]],
      { failFirstAssistantSave: true },
    );
    const ran = await r.chat.run({ prompt: 'go' });
    await r.next(); // the tool asks for approval, then the step cannot be saved
    expect(r.events(ran.threadId, 'INPUT_REQUIRED')).toHaveLength(0);
    expect(await r.state(ran.threadId)).toBe('QUEUED'); // the retry policy has it
    await r.next();
    expect(await r.state(ran.threadId)).toBe('COMPLETED');
    expect(r.sent).toEqual([]);
  });

  it('the other results of a step that parks are kept', async () => {
    const r = await setup([[call('l1', 'lookup', {}), call('a1', 'send', {}), finish('tool-calls')]]);
    const ran = await r.chat.run({ prompt: 'go' });
    await r.next();
    const results = r.rows(ran.threadId)
      .filter((m) => m.role === 'tool')
      .flatMap((m) => m.content as any[]);
    expect(results.map((p) => p.toolCallId)).toEqual(['l1']); // a1 has no result yet
    expect(results[0].result).toEqual({ found: 42 });
  });
});

describe('an answer (§2.5)', () => {
  it('an approved tool that ran before a crash is not run again', async () => {
    const r = await setup([[call('a1', 'send', {}), finish('tool-calls')], [say('done'), finish('stop')]]);
    const ran = await r.chat.run({ prompt: 'go' });
    await r.next();
    r.queue.items.shift(); // the park's expiry job
    expect((await r.runtime.hitl.respond({ threadId: ran.threadId, toolCallId: 'a1', approved: true })).delivered).toBe(true);
    // The tool ran, then the worker died before its result was saved.
    await r.kv.set(hitlDoneKey('a1'), JSON.stringify({ cached: true }));
    await r.next();
    expect(r.sent).toEqual([]); // not run a second time
    expect(await r.state(ran.threadId)).toBe('COMPLETED');
    const result = (r.rows(ran.threadId)[2]!.content as any[])[0];
    expect(result).toMatchObject({ toolCallId: 'a1', result: { cached: true } });
    expect(await r.kv.get(hitlKey('a1'))).toBeNull(); // cleared once landed
    expect(await r.kv.get(hitlDoneKey('a1'))).toBeNull();
  });

  it('a second answer is refused', async () => {
    const r = await setup([[call('a1', 'send', {}), finish('tool-calls')]]);
    const ran = await r.chat.run({ prompt: 'go' });
    await r.next();
    const first = await r.runtime.hitl.respond({ threadId: ran.threadId, toolCallId: 'a1', approved: true });
    const second = await r.runtime.hitl.respond({ threadId: ran.threadId, toolCallId: 'a1', approved: false });
    expect(first.delivered).toBe(true);
    expect(second).toEqual({ delivered: false, error: 'This request was already answered' });
  });

  it("an earlier run's park is not open", async () => {
    const r = await setup([[call('a1', 'send', {}), finish('tool-calls')]]);
    const ran = await r.chat.run({ prompt: 'go' });
    await r.next();
    expect(await loadOpenHitls(r.ports, ran.threadId)).toHaveLength(1);
    await r.kv.set(runIdKey(ran.threadId), 'a-later-run');
    expect(await loadOpenHitls(r.ports, ran.threadId)).toHaveLength(0);
  });
});

/** The Go nested script: the parent spawns, the child asks to wipe prod. */
const nestedSteps = (...after: Array<LanguageModelV1StreamPart[] | Error>) => [
  [call('s1', 'spawnSubagent', { name: 'researcher', instructions: 'Find the answer and report back.' }), finish('tool-calls')],
  [call('d1', 'wipe', { target: 'prod' }), finish('tool-calls')],
  ...after,
];

describe('unwinding a nested park (§2.7)', () => {
  it('a child that fails while unwinding is reported to its parent', async () => {
    const r = await setup(nestedSteps(new Error('boom'), [say('parent: handled'), finish('stop')]), { subagents: true });
    const ran = await r.chat.run({ prompt: 'go' });
    await r.next();
    r.queue.items.shift(); // expiry job
    await r.runtime.hitl.respond({ threadId: ran.threadId, toolCallId: 'd1', approved: true });
    await r.next();
    expect(r.wiped).toEqual(['prod']);
    expect(await r.state(ran.threadId)).toBe('COMPLETED'); // not stuck
    expect(r.events(ran.threadId, 'SUBAGENT_FAILED')).toHaveLength(1);
    const spawn = (r.rows(ran.threadId)[2]!.content as any[])[0];
    expect(spawn.toolCallId).toBe('s1');
    expect(spawn.result.error).toBeTruthy();
  });

  it('an unwind cut short carries on from the level still waiting', async () => {
    const r = await setup(
      nestedSteps([say('child: done'), finish('stop')], [say('parent: done'), finish('stop')]),
      { subagents: true },
    );
    const ran = await r.chat.run({ prompt: 'go' });
    await r.next();
    const childId = (r.events(ran.threadId, 'SUBAGENT_STARTED')[0]!.payload as any).agentId;
    // A worker landed the child's verdict, then died before the level above.
    await r.storage.messages.append(ran.threadId, {
      role: 'tool',
      agentId: childId,
      content: [{ type: 'tool-result', toolCallId: 'd1', toolName: 'wipe', result: { denied: true } }],
    });
    await r.next(); // the park's expiry job comes due
    expect(r.wiped).toEqual([]); // the landed verdict is not settled again
    expect(await r.state(ran.threadId)).toBe('COMPLETED');
    expect((r.rows(ran.threadId)[2]!.content as any[])[0].toolCallId).toBe('s1');
  });
});

describe('tools (§2.5)', () => {
  it('a tool that throws hands the error to the model', async () => {
    const r = await setup([[call('l1', 'lookup', { bad: true }), finish('tool-calls')], [say('sorry'), finish('stop')]]);
    const ran = await r.chat.run({ prompt: 'go' });
    await r.next();
    expect(await r.state(ran.threadId)).toBe('COMPLETED'); // the run goes on
    const last = r.prompts.at(-1)!;
    const results = last.filter((m: any) => m.role === 'tool').flatMap((m: any) => m.content);
    expect(results.some((p: any) => p.result === 'error: bad input')).toBe(true);
  });

  it('an orphan tool result is dropped', () => {
    const out = repairDanglingToolCalls([
      { role: 'user', content: 'hi' },
      { role: 'tool', content: [{ type: 'tool-result', toolCallId: 'gone', toolName: 'x', result: {} }] },
      { role: 'assistant', content: 'hello' },
    ] as any[]);
    expect(out.map((m: any) => m.role)).toEqual(['user', 'assistant']);
  });
});
