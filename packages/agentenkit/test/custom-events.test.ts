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
import { publishEvent, RESERVED_EVENT_TYPES } from '../src/core/publish.js';
import { resolveConfig } from '../src/core/types.js';
import { bindStorage } from '../src/core/state.js';
import type { RuntimeOptions } from '../src/ports/runtime.js';

interface ScriptedStep {
  text?: string;
  toolCalls?: Array<{ toolCallId: string; toolName: string; args: any }>;
}

/** One scripted step per round-trip, like the engine-loop suite. */
function scriptedModel(steps: ScriptedStep[]) {
  let call = 0;
  return new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock',
    doStream: async () => {
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
}

async function makeRuntime(model: any) {
  const storage = new MemoryStorage();
  const bus = new MemoryBus();
  const queue = new MemoryQueue();
  const kv = new MemoryKv();
  const deps: RuntimeOptions = {
    storage, admin: new MemoryAdminStore(), bus, queue, kv,
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
    config: resolveConfig(),
  };
  return { runtime: await setupAgentCore(deps), storage, bus, queue, kv, deps };
}

const ofType = (bus: MemoryBus, type: string) => bus.published.filter((e) => e.type === type);

describe('custom events', () => {
  it('a tool publishes a durable event: logged, fanned out, replayed', async () => {
    const r = await makeRuntime(
      scriptedModel([{ toolCalls: [{ toolCallId: 'c1', toolName: 'render', args: {} }] }, { text: 'done' }]),
    );
    const chat = r.runtime.createStreamTextAgent({
      name: 'chat', model: 'gpt-4o',
      tools: {
        render: agentTool({
          parameters: z.object({}),
          execute: async (_args, { publishEvent, state }) => {
            const event = await publishEvent('DESIGN_PREVIEW', { url: 'https://x/1.png', org: state.orgId });
            expect(event.seq).toBeGreaterThan(0);
            return { ok: true };
          },
        }),
      },
    });
    const ran = await chat.run({ prompt: 'hi', state: { orgId: 'acme' } });
    await r.runtime.worker.handleJob(r.queue.items[0]!);

    const live = ofType(r.bus, 'DESIGN_PREVIEW');
    expect(live).toHaveLength(1);
    expect(live[0]!.payload).toEqual({ url: 'https://x/1.png', org: 'acme' });
    // Durable: it is in the log, in order, and a reconnecting client replays it
    const logged = await r.runtime.events.since(ran.threadId, -1);
    const preview = logged.find((e) => e.type === 'DESIGN_PREVIEW')!;
    expect(preview.seq).toBe(live[0]!.seq);
    const snap = await r.runtime.getThreadSnapshot(ran.threadId);
    // The run finished, so activeEvents is empty; the full log still has it
    expect(snap!.lastEventSeq).toBeGreaterThanOrEqual(preview.seq);
  });

  it('a notice reaches the bus only, with seq 0', async () => {
    const r = await makeRuntime(
      scriptedModel([{ toolCalls: [{ toolCallId: 'c1', toolName: 'slow', args: {} }] }, { text: 'done' }]),
    );
    const chat = r.runtime.createStreamTextAgent({
      name: 'chat', model: 'gpt-4o',
      tools: {
        slow: agentTool({
          parameters: z.object({}),
          execute: async (_args, { publishEvent }) => {
            await publishEvent('PROGRESS', { label: 'Rendering…' }, { durable: false });
            return 'ok';
          },
        }),
      },
    });
    const ran = await chat.run({ prompt: 'hi' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);
    const notices = ofType(r.bus, 'PROGRESS');
    expect(notices).toHaveLength(1);
    expect(notices[0]!.seq).toBe(0);
    const logged = await r.runtime.events.since(ran.threadId, -1);
    expect(logged.some((e) => e.type === 'PROGRESS')).toBe(false);
  });

  it('refuses a platform event type', async () => {
    const r = await makeRuntime(scriptedModel([{ text: 'ok' }]));
    const deps = { ...r.deps, storage: bindStorage(r.storage, { state: {} }), admin: r.deps.admin!, config: r.deps.config! } as any;
    for (const type of ['STATE_CHANGE', 'CHUNK', 'INPUT_REQUIRED']) {
      expect(RESERVED_EVENT_TYPES.has(type)).toBe(true);
      await expect(publishEvent(deps, 't', type, {})).rejects.toThrow(/platform event type/);
    }
    await expect(publishEvent(deps, 't', '', {})).rejects.toThrow(/type is required/);
  });

  it('the runtime publishes from outside a run, scoped by state', async () => {
    const r = await makeRuntime(scriptedModel([{ text: 'ok' }]));
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const ran = await chat.run({ prompt: 'hi' });
    const event = await r.runtime.events.publishEvent(ran.threadId, 'BILLING', { credits: 0 }, { state: { orgId: 'acme' } });
    expect(event.type).toBe('BILLING');
    expect(ofType(r.bus, 'BILLING')).toHaveLength(1);
    const logged = await r.runtime.events.since(ran.threadId, -1);
    expect(logged.at(-1)!.type).toBe('BILLING');
  });

  it('a nested run and a resumed approval get it too', async () => {
    const seen: string[] = [];
    const r = await makeRuntime(
      scriptedModel([
        { toolCalls: [{ toolCallId: 's1', toolName: 'spawnSubagent', args: { name: 'kid', instructions: 'go' } }] },
        { toolCalls: [{ toolCallId: 'd1', toolName: 'wipe', args: {} }] }, // the child parks
        { text: 'child done' },
        { text: 'parent done' },
      ]),
    );
    const chat = r.runtime.createStreamTextAgent({
      name: 'chat', model: 'gpt-4o',
      subagents: {
        tools: {
          wipe: markRequiresConfirmation(
            agentTool({
              parameters: z.object({}),
              execute: async (_args, { publishEvent, state }) => {
                seen.push(`wipe:${state.orgId}`);
                await publishEvent('WIPED', { by: 'kid' });
                return 'gone';
              },
            }),
          ),
        },
      },
    });
    const ran = await chat.run({ prompt: 'hi', state: { orgId: 'acme' } });
    await r.runtime.worker.handleJob(r.queue.items[0]!);
    await r.runtime.hitl.respond({ threadId: ran.threadId, toolCallId: 'd1', approved: true });
    await r.queue.drain((job) => r.runtime.worker.handleJob(job).then(() => undefined));
    expect(seen).toEqual(['wipe:acme']);
    expect(ofType(r.bus, 'WIPED')).toHaveLength(1);
    expect(ofType(r.bus, 'STATE_CHANGE').at(-1)!.payload).toMatchObject({ state: 'COMPLETED' });
  });
});

describe('approval payload', () => {
  it('reaches the approved tool as `approval.payload`', async () => {
    let seen: unknown = 'unset';
    let firstCall: unknown = 'unset';
    const r = await makeRuntime(
      scriptedModel([{ toolCalls: [{ toolCallId: 'q1', toolName: 'askQuestions', args: { questions: ['Colour?'] } }] }, { text: 'thanks' }]),
    );
    const chat = r.runtime.createStreamTextAgent({
      name: 'chat', model: 'gpt-4o',
      tools: {
        askQuestions: markRequiresConfirmation(
          agentTool({
            parameters: z.object({ questions: z.array(z.string()) }),
            execute: async (_args, { approval }) => {
              seen = approval?.payload;
              return { answers: approval?.payload };
            },
          }),
        ),
      },
    });
    const ran = await chat.run({ prompt: 'hi' });
    await r.runtime.worker.handleJob(r.queue.items[0]!);
    firstCall = seen; // parked: never executed
    await r.runtime.hitl.respond({ threadId: ran.threadId, toolCallId: 'q1', approved: true, payload: { Colour: 'teal' } });
    await r.queue.drain((job) => r.runtime.worker.handleJob(job).then(() => undefined));
    expect(firstCall).toBe('unset');
    expect(seen).toEqual({ Colour: 'teal' });
  });
});
