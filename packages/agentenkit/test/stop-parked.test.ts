import { describe, expect, it } from 'bun:test';
import { simulateReadableStream } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { z } from 'zod';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { markRequiresConfirmation } from '../src/core/engine.js';
import { agentTool } from '../src/core/tools.js';
import { repairDanglingToolCalls } from '../src/core/messages.js';
import { resolveConfig } from '../src/core/types.js';
import type { RuntimeOptions } from '../src/ports/runtime.js';

interface ScriptedStep {
  text?: string;
  toolCalls?: Array<{ toolCallId: string; toolName: string; args: any }>;
}

/** Records every prompt it is sent, so a test can prove the history a run
 *  carries is well-formed. */
function scriptedModel(steps: ScriptedStep[]) {
  let call = 0;
  const prompts: any[] = [];
  const model = new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock',
    doStream: async (options) => {
      prompts.push(options.prompt);
      const step = steps[Math.min(call++, steps.length - 1)]!;
      const chunks: LanguageModelV1StreamPart[] = [];
      if (step.text) chunks.push({ type: 'text-delta', textDelta: step.text });
      for (const tc of step.toolCalls ?? []) {
        chunks.push({
          type: 'tool-call', toolCallType: 'function',
          toolCallId: tc.toolCallId, toolName: tc.toolName, args: JSON.stringify(tc.args),
        });
      }
      chunks.push({
        type: 'finish',
        finishReason: (step.toolCalls?.length ?? 0) > 0 ? 'tool-calls' : 'stop',
        usage: { promptTokens: 10, completionTokens: 5 },
      });
      return { stream: simulateReadableStream({ chunks }), rawCall: { rawPrompt: null, rawSettings: {} } };
    },
  });
  return { model, prompts };
}

async function makeRuntime(model: any) {
  const storage = new MemoryStorage();
  const bus = new MemoryBus();
  const queue = new MemoryQueue();
  const deps: RuntimeOptions = {
    storage, admin: new MemoryAdminStore(), bus, queue, kv: new MemoryKv(),
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
    config: resolveConfig(),
  };
  return { runtime: await setupAgentCore(deps), storage, bus, queue, admin: deps.admin as MemoryAdminStore };
}

const roles = (storage: MemoryStorage, threadId: string, agentId: string | null = null) =>
  storage.messages.store.get(threadId)!.filter((m) => (m.agentId ?? null) === agentId).map((m) => m.role);

/** Every tool call in a prompt must have a result somewhere after it. */
function danglingCalls(prompt: any[]): string[] {
  const dangling: string[] = [];
  prompt.forEach((m, i) => {
    if (m.role !== 'assistant' || !Array.isArray(m.content)) return;
    for (const p of m.content) {
      if (p.type !== 'tool-call') continue;
      const answered = prompt.slice(i + 1).some(
        (later) => Array.isArray(later.content) &&
          later.content.some((q: any) => q.type === 'tool-result' && q.toolCallId === p.toolCallId),
      );
      if (!answered) dangling.push(p.toolCallId);
    }
  });
  return dangling;
}

describe('stop while parked (§2.5)', () => {
  it('closes the dangling tool call, so the next run sends a well-formed prompt', async () => {
    const executed: string[] = [];
    const { model, prompts } = scriptedModel([
      { toolCalls: [{ toolCallId: 'c1', toolName: 'wipe', args: {} }] },
      { text: 'after' },
    ]);
    const r = await makeRuntime(model);
    const chat = r.runtime.createStreamTextAgent({
      name: 'chat', model: 'gpt-4o',
      tools: {
        wipe: markRequiresConfirmation(agentTool({
          parameters: z.object({}),
          execute: async () => { executed.push('ran'); return 'gone'; },
        })),
      },
    });
    const ran = await chat.run({ prompt: 'go' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);
    expect(roles(r.storage, ran.threadId)).toEqual(['user', 'assistant']); // parked: no result yet

    expect((await chat.stop(ran.threadId)).accepted).toBe(true);
    expect(roles(r.storage, ran.threadId)).toEqual(['user', 'assistant', 'tool']);
    const closed = r.storage.messages.store.get(ran.threadId)!.at(-1)!.content as any[];
    expect(closed[0]).toMatchObject({ type: 'tool-result', toolCallId: 'c1', result: { cancelled: true, reason: 'stopped' } });
    expect(executed).toEqual([]);

    // The park is settled: not answerable any more, and the expiry job is a no-op
    expect((await r.runtime.hitl.respond({ threadId: ran.threadId, toolCallId: 'c1', approved: true })).delivered).toBe(false);
    await r.queue.drain((job) => r.runtime.worker.handleJob(job).then(() => undefined));
    expect(r.storage.threads.store.get(ran.threadId)!.state).toBe('CANCELLED');

    // The next run's prompt has a result for every call
    await chat.run({ threadId: ran.threadId, prompt: 'again' });
    await r.queue.drain((job) => r.runtime.worker.handleJob(job).then(() => undefined));
    expect(danglingCalls(prompts.at(-1)!)).toEqual([]);
  });

  it('closes a nested park\'s whole chain, and cancels the child\'s record', async () => {
    const { model } = scriptedModel([
      { toolCalls: [{ toolCallId: 's1', toolName: 'spawnSubagent', args: { name: 'kid', instructions: 'go' } }] },
      { toolCalls: [{ toolCallId: 'd1', toolName: 'wipe', args: {} }] },
    ]);
    const r = await makeRuntime(model);
    const chat = r.runtime.createStreamTextAgent({
      name: 'chat', model: 'gpt-4o',
      subagents: {
        tools: { wipe: markRequiresConfirmation(agentTool({ parameters: z.object({}), execute: async () => 'gone' })) },
      },
    });
    const ran = await chat.run({ prompt: 'go' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);
    const childId = (r.bus.published.find((e) => e.type === 'SUBAGENT_STARTED')!.payload as any).agentId as string;

    await chat.stop(ran.threadId);
    expect(roles(r.storage, ran.threadId)).toEqual(['user', 'assistant', 'tool']);
    expect((r.storage.messages.store.get(ran.threadId)!.at(-1)!.content as any[])[0].toolCallId).toBe('s1');
    expect(roles(r.storage, ran.threadId, childId)).toEqual(['user', 'assistant', 'tool']);
    expect((await r.admin.runs.get(childId))!.state).toBe('CANCELLED');
  });
});

describe('repairDanglingToolCalls', () => {
  it('inserts a result for the unanswered call only, before the next turn', () => {
    const history = [
      { role: 'user', content: 'hi' },
      { role: 'assistant', content: [
        { type: 'tool-call', toolCallId: 'a', toolName: 'x', args: {} },
        { type: 'tool-call', toolCallId: 'b', toolName: 'y', args: {} },
      ] },
      { role: 'tool', content: [{ type: 'tool-result', toolCallId: 'a', toolName: 'x', result: 'ok' }] },
      { role: 'user', content: 'again' },
    ];
    const out = repairDanglingToolCalls(history);
    expect(out).toHaveLength(5);
    expect(out[3]).toMatchObject({ role: 'tool', content: [{ type: 'tool-result', toolCallId: 'b' }] });
    expect(out[4]).toEqual(history[3]);
    expect(repairDanglingToolCalls(out)).toHaveLength(5); // idempotent
    expect(danglingCalls(out)).toEqual([]);
  });
});
