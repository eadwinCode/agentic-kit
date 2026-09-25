import { describe, expect, it } from 'bun:test';
import { simulateReadableStream } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { z } from 'zod';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryRunStreams, MemoryStorage } from '../src/adapters/memory.js';
import { markRequiresConfirmation } from '../src/core/engine.js';
import { agentTool } from '../src/core/tools.js';
import { openSegment } from '../src/core/segment.js';
import { resolveConfig } from '../src/core/types.js';
import type { StreamItem } from '../src/core/stream-events.js';

// Workstream S4: the engine writes a run stream per segment. The Go package
// runs the same cases under the same names (segment_streams_test.go).

interface ScriptedStep {
  reasoning?: string;
  text?: string[];
  toolCalls?: Array<{ toolCallId: string; toolName: string; args: any }>;
  fail?: boolean;
}

function scriptedModel(steps: ScriptedStep[]) {
  let call = 0;
  return new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock',
    doStream: async () => {
      const step = steps[Math.min(call++, steps.length - 1)]!;
      if (step.fail) throw new Error('provider down');
      const chunks: LanguageModelV1StreamPart[] = [];
      if (step.reasoning) chunks.push({ type: 'reasoning', textDelta: step.reasoning });
      for (const t of step.text ?? []) chunks.push({ type: 'text-delta', textDelta: t });
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
    // A one-shot agent asks once, without a stream.
    doGenerate: async () => {
      const step = steps[Math.min(call++, steps.length - 1)]!;
      return {
        text: (step.text ?? []).join(''), finishReason: 'stop',
        usage: { promptTokens: 10, completionTokens: 5 }, rawCall: { rawPrompt: null, rawSettings: {} },
      };
    },
  });
}

async function makeRuntime(model: any, config: Record<string, unknown> = {}) {
  const queue = new MemoryQueue();
  const streams = new MemoryRunStreams();
  const runtime = await setupAgentCore({
    storage: new MemoryStorage(), admin: new MemoryAdminStore(), bus: new MemoryBus(), queue, kv: new MemoryKv(),
    streams,
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
    config: resolveConfig({ streamFlushMs: 0, runMaxAttempts: 1, ...config }),
  });
  return { runtime, queue, streams };
}

async function items(streams: MemoryRunStreams, streamId: string): Promise<StreamItem[]> {
  return (await streams.snapshot(streamId))?.items ?? [];
}

const types = (list: StreamItem[]) => list.map((i) => i.type);

describe('segment streams', () => {
  it('a run writes its stream from RUN_STARTED to RUN_FINISHED', async () => {
    const r = await makeRuntime(scriptedModel([{ reasoning: 'hmm', text: ['Hel', 'lo'] }]));
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const ran = await chat.run({ prompt: 'hi' });
    await r.runtime.worker.handleJob(r.queue.items.shift()!);

    const got = await items(r.streams, `${ran.runId}:1`);
    expect(types(got)).toEqual([
      'RUN_STARTED',
      'REASONING_START', 'REASONING_CONTENT', 'REASONING_END',
      'TEXT_MESSAGE_START', 'TEXT_MESSAGE_CONTENT', 'TEXT_MESSAGE_END',
      'STEP_FINISHED', 'RUN_FINISHED',
    ]);
    expect(got[0]).toMatchObject({ type: 'RUN_STARTED', threadId: ran.threadId, runId: ran.runId, segment: 1 });
    const text = got.filter((i) => i.type === 'TEXT_MESSAGE_CONTENT').map((i: any) => i.delta).join('');
    expect(text).toBe('Hello');
    expect(got.at(-2)).toMatchObject({ type: 'STEP_FINISHED', step: 1, agentId: null, finishReason: 'stop' });
    expect(got.at(-1)).toMatchObject({
      type: 'RUN_FINISHED', status: 'finished', finishReason: 'stop', usage: { totalTokens: 15 },
    });
    expect((await r.streams.snapshot(`${ran.runId}:1`))!.end).toMatchObject({ status: 'finished' });
  });

  it('tool calls stream as TOOL_CALL events', async () => {
    const r = await makeRuntime(scriptedModel([
      { toolCalls: [{ toolCallId: 'c1', toolName: 'lookup', args: { q: 'x' } }] },
      { text: ['found'] },
    ]));
    const chat = r.runtime.createStreamTextAgent({
      name: 'chat', model: 'gpt-4o',
      tools: { lookup: agentTool({ parameters: z.object({ q: z.string() }), execute: async ({ q }) => `result for ${q}` }) },
    });
    const ran = await chat.run({ prompt: 'hi' });
    await r.runtime.worker.handleJob(r.queue.items.shift()!);

    const got = await items(r.streams, `${ran.runId}:1`);
    expect(types(got)).toEqual([
      'RUN_STARTED',
      'TOOL_CALL_START', 'TOOL_CALL_END', 'TOOL_CALL_RESULT', 'STEP_FINISHED',
      'TEXT_MESSAGE_START', 'TEXT_MESSAGE_CONTENT', 'TEXT_MESSAGE_END', 'STEP_FINISHED',
      'RUN_FINISHED',
    ]);
    expect(got[2]).toMatchObject({ type: 'TOOL_CALL_END', toolCallId: 'c1', toolName: 'lookup', args: { q: 'x' } });
    expect(got[3]).toMatchObject({ type: 'TOOL_CALL_RESULT', toolCallId: 'c1', result: 'result for x' });
  });

  it('publishEvent arrives as CUSTOM with its name and value', async () => {
    const r = await makeRuntime(scriptedModel([
      { toolCalls: [{ toolCallId: 'c1', toolName: 'render', args: {} }] },
      { text: ['done'] },
    ]));
    const chat = r.runtime.createStreamTextAgent({
      name: 'chat', model: 'gpt-4o',
      tools: {
        render: agentTool({
          parameters: z.object({}),
          execute: async (_args, { publishEvent }) => {
            await publishEvent('SEARCH_PROGRESS', { done: 3, of: 10 }, { durable: false });
            return 'ok';
          },
        }),
      },
    });
    const ran = await chat.run({ prompt: 'hi' });
    await r.runtime.worker.handleJob(r.queue.items.shift()!);
    const custom = (await items(r.streams, `${ran.runId}:1`)).filter((i) => i.type === 'CUSTOM');
    expect(custom).toEqual([{ type: 'CUSTOM', name: 'SEARCH_PROGRESS', value: { done: 3, of: 10 }, offset: expect.any(String) }]);
  });

  it('a park closes the segment and the resume opens the next', async () => {
    const r = await makeRuntime(scriptedModel([
      { toolCalls: [{ toolCallId: 'c1', toolName: 'wipe', args: {} }] },
      { text: ['wiped'] },
    ]), { hitlTtlMs: 60 * 60_000 });
    const wipe = markRequiresConfirmation(agentTool({ parameters: z.object({}), execute: async () => 'gone' }));
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o', tools: { wipe } });
    const ran = await chat.run({ prompt: 'wipe it' });
    await r.runtime.worker.handleJob(r.queue.items.shift()!);

    const first = await items(r.streams, `${ran.runId}:1`);
    expect(first.some((i) => i.type === 'INPUT_REQUIRED' && (i as any).toolCallId === 'c1')).toBe(true);
    expect(first.at(-1)).toMatchObject({ type: 'RUN_FINISHED', status: 'parked' });

    await r.runtime.hitl.respond({ threadId: ran.threadId, toolCallId: 'c1', approved: true });
    const resume = r.queue.items.findIndex((j) => r.queue.keyOf(j) === 'hitl-resume:c1');
    await r.runtime.worker.handleJob(r.queue.items.splice(resume, 1)[0]!);

    const second = await items(r.streams, `${ran.runId}:2`);
    expect(second[0]).toMatchObject({ type: 'RUN_STARTED', segment: 2, streamId: `${ran.runId}:2` });
    expect(second.some((i) => i.type === 'TOOL_CALL_RESULT' && (i as any).toolCallId === 'c1')).toBe(true);
    expect(second.at(-1)).toMatchObject({ type: 'RUN_FINISHED', status: 'finished' });
  });

  it('a failed run ends its stream with RUN_ERROR', async () => {
    const r = await makeRuntime(scriptedModel([{ fail: true }]));
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const ran = await chat.run({ prompt: 'hi' });
    await r.runtime.worker.handleJob(r.queue.items.shift()!).catch(() => undefined);
    const got = await items(r.streams, `${ran.runId}:1`);
    expect(got.at(-1)).toMatchObject({ type: 'RUN_ERROR', status: 'error' });
    expect((got.at(-1) as any).error).toContain('provider down');
  });

  it("a one-shot agent's text rides on RUN_FINISHED", async () => {
    const r = await makeRuntime(scriptedModel([{ text: ['forty-two'] }]));
    const oneShot = r.runtime.createGenerateTextAgent({ name: 'answer', model: 'gpt-4o' });
    const ran = await oneShot.run({ prompt: 'q' });
    await r.runtime.worker.handleJob(r.queue.items.shift()!);
    const got = await items(r.streams, `${ran.runId}:1`);
    expect(got.at(-1)).toMatchObject({ type: 'RUN_FINISHED', status: 'finished', text: 'forty-two' });
  });

  it("a lost worker's stream is closed as lost by the sweep", async () => {
    const r = await makeRuntime(scriptedModel([{ text: ['never'] }]));
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const ran = await chat.run({ prompt: 'hi' });
    const job = r.queue.items.shift()!;
    // A worker picked the run up, opened its stream and died.
    await openSegment(r.runtime.ports(), ran.threadId, ran.runId);
    await r.runtime.worker.handleDeadJob(job, 3, new Error('worker died'));
    const snap = await r.streams.snapshot(`${ran.runId}:1`);
    expect(snap!.end).toMatchObject({ type: 'RUN_ERROR', status: 'lost' });
  });

  it('a subagent run is wrapped in SUBAGENT_EVENT', async () => {
    const isChild = (prompt: any) =>
      (prompt ?? []).some((m: any) => m.role === 'system' && JSON.stringify(m.content).includes('subagent'));
    const answered = (prompt: any) =>
      (prompt ?? []).some((m: any) => m.role === 'tool' && JSON.stringify(m.content).includes('spawnSubagent'));
    const reply = (chunks: LanguageModelV1StreamPart[]) => ({
      stream: simulateReadableStream({ chunks }), rawCall: { rawPrompt: null, rawSettings: {} },
    });
    const finish = (finishReason: 'stop' | 'tool-calls'): LanguageModelV1StreamPart =>
      ({ type: 'finish', finishReason, usage: { promptTokens: 10, completionTokens: 5 } });
    const model = new MockLanguageModelV1({
      provider: 'mock',
      modelId: 'mock',
      doStream: async ({ prompt }: any) => {
        if (isChild(prompt)) return reply([{ type: 'text-delta', textDelta: 'did it' }, finish('stop')]);
        if (answered(prompt)) return reply([{ type: 'text-delta', textDelta: 'all set' }, finish('stop')]);
        return reply([
          {
            type: 'tool-call', toolCallType: 'function', toolCallId: 'p1', toolName: 'spawnSubagent',
            args: JSON.stringify({ name: 'helper', instructions: 'do it' }),
          },
          finish('tool-calls'),
        ]);
      },
    });
    const r = await makeRuntime(model);
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o', subagents: true });
    const ran = await chat.run({ prompt: 'delegate' });
    await r.runtime.worker.handleJob(r.queue.items.shift()!);

    const got = await items(r.streams, `${ran.runId}:1`);
    const started = got.find((i) => i.type === 'SUBAGENT_STARTED') as any;
    expect(started).toMatchObject({ name: 'helper', depth: 1 });
    const nested = got.filter((i) => i.type === 'SUBAGENT_EVENT') as any[];
    expect(nested.every((i) => i.subagentId === started.subagentId)).toBe(true);
    expect(nested.map((i) => i.event.type)).toEqual(['TEXT_MESSAGE_START', 'TEXT_MESSAGE_CONTENT', 'TEXT_MESSAGE_END']);
    expect(got.some((i) => i.type === 'STEP_FINISHED' && (i as any).agentId === started.subagentId)).toBe(true);
    expect(got.find((i) => i.type === 'SUBAGENT_FINISHED')).toMatchObject({ subagentId: started.subagentId, status: 'completed' });
    expect(got.at(-1)).toMatchObject({ type: 'RUN_FINISHED', status: 'finished' });
  });

  it('events wait for the flush window, and a step end goes out at once', async () => {
    const r = await makeRuntime(scriptedModel([{ text: ['a'] }]), { streamFlushMs: 60_000, streamFlushEvents: 1000 });
    const seg = (await openSegment(r.runtime.ports(), 't1', 'run1'))!;
    await seg.forward('CHUNK', { type: 'text-delta', textDelta: 'a' }, true);
    expect(types(await items(r.streams, 'run1:1'))).toEqual(['RUN_STARTED']); // held
    await seg.forward('STEP_FINISHED', { agentId: null, index: 1, finishReason: 'stop', totalTokens: 3 }, true);
    expect(types(await items(r.streams, 'run1:1'))).toEqual([
      'RUN_STARTED', 'TEXT_MESSAGE_START', 'TEXT_MESSAGE_CONTENT', 'TEXT_MESSAGE_END', 'STEP_FINISHED',
    ]);
    await seg.close({ type: 'RUN_FINISHED', status: 'finished' });
    expect(seg.closed).toBe(true);
  });
});
