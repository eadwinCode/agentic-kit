import { describe, expect, it } from 'bun:test';
import { simulateReadableStream } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { bindStorage } from '../src/core/state.js';
import { compactContext } from '../src/core/context.js';
import { promptHistory } from '../src/core/messages.js';
import { resolveConfig } from '../src/core/types.js';
import type { RuntimePorts } from '../src/ports/runtime.js';

// Workstream E: a summary covers what it summarized, so the prompt stops
// carrying it, a thread compacts once per time it outgrows the trigger, and
// the kept tail starts at a user turn. The same cases run in the Go package
// (compaction_test.go).

/** Answers every streamed call "ok" and every summary request "summary", and
 *  keeps the prompts it streamed. */
function summarizingModel() {
  const prompts: any[] = [];
  const model = new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock-summarizing',
    doGenerate: async () => ({
      text: 'summary',
      finishReason: 'stop',
      usage: { promptTokens: 100, completionTokens: 10 },
      rawCall: { rawPrompt: null, rawSettings: {} },
    }),
    doStream: async ({ prompt }: any) => {
      prompts.push(prompt);
      const chunks: LanguageModelV1StreamPart[] = [
        { type: 'text-delta', textDelta: 'ok' },
        { type: 'finish', finishReason: 'stop', usage: { promptTokens: 10, completionTokens: 5 } },
      ];
      return { stream: simulateReadableStream({ chunks }), rawCall: { rawPrompt: null, rawSettings: {} } };
    },
  });
  return { model, lastPrompt: () => JSON.stringify(prompts.at(-1)) };
}

/** A 1,800-token budget, compacting past 1,440, keeping a 450-token tail.
 *  Each prompt below is about 300 tokens. */
async function compactingRuntime() {
  const { model, lastPrompt } = summarizingModel();
  const storage = new MemoryStorage();
  const queue = new MemoryQueue();
  const bus = new MemoryBus();
  const kv = new MemoryKv();
  const admin = new MemoryAdminStore();
  const config = resolveConfig({
    contextCeilingTokens: 2_000,
    contextOutputReserveTokens: 200,
    compactionModel: 'gpt-4o',
    promptCaching: false,
  });
  const resolveModel = () => ({ instance: () => model, contextWindow: 128_000 });
  const runtime = await setupAgentCore({ storage, queue, bus, kv, admin, config, resolveModel });
  const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
  const ports: RuntimePorts = {
    storage: bindStorage(storage, { state: {} }), queue, bus, kv, admin, config, resolveModel,
  };
  const runPrompt = async (threadId: string | undefined, i: number) => {
    const ran = await chat.run({ ...(threadId ? { threadId } : {}), prompt: `run-${i} ${'x'.repeat(1_200)}` });
    await runtime.worker.handleJob(queue.items.shift()!);
    return ran;
  };
  const summaries = (threadId: string) =>
    storage.messages.store.get(threadId)!.filter((m) => (m.content as any)?.type === 'CONTEXT_SUMMARY').length;
  return { storage, ports, lastPrompt, runPrompt, summaries };
}

describe('compaction (§2.6)', () => {
  it('a thread compacts once each time it grows past the trigger', async () => {
    const r = await compactingRuntime();
    const first = await r.runPrompt(undefined, 1);
    for (let i = 2; i <= 6; i++) await r.runPrompt(first.threadId, i);
    expect(r.summaries(first.threadId)).toBe(1); // not on every run after
    expect(r.lastPrompt()).not.toContain('run-1 '); // a summarized turn is not sent again
    expect(r.lastPrompt()).toContain('run-6 ');
  });

  it('the summary is billed to the run it served', async () => {
    const r = await compactingRuntime();
    const first = await r.runPrompt(undefined, 1);
    let compacting = first;
    for (let i = 2; i <= 5; i++) compacting = await r.runPrompt(first.threadId, i);
    const row = r.storage.usage.recorded.find((u) => u.kind === 'compaction');
    expect(row?.runId).toBe(compacting.runId);
  });

  it('the kept tail starts at a user turn', async () => {
    const r = await compactingRuntime();
    const th = await r.storage.threads.create({});
    const add = (role: string, content: unknown) => r.storage.messages.append(th.id, { role: role as any, content });
    const big = 'y'.repeat(2_400);
    await add('user', `first ${big}`);
    await add('user', `second ${big}`);
    await add('assistant', [
      { type: 'tool-call', toolCallId: 'c1', toolName: 'lookup', args: { q: 'q'.repeat(300) } },
    ]);
    // A result that fits the tail on its own, with its call just outside it.
    await add('tool', [{ type: 'tool-result', toolCallId: 'c1', toolName: 'lookup', result: 'z'.repeat(1_300) }]);
    await add('assistant', 'looked it up');
    await add('user', 'third');
    const out = await compactContext(r.ports, th.id, 'gpt-4o');
    expect(out.map((m) => m.role)).toEqual(['system', 'user']); // summary + the last user turn
  });

  it('a summary from before the cover mark is left out', () => {
    const msg = (id: string, role: string, content: unknown) => ({ id, role, content });
    const u1 = msg('u1', 'user', 'one');
    const a1 = msg('a1', 'assistant', 'two');
    const u2 = msg('u2', 'user', 'three');
    const legacy = msg('s0', 'system', { type: 'CONTEXT_SUMMARY', text: 'old' });
    const marked = msg('s1', 'system', { type: 'CONTEXT_SUMMARY', text: 'new', coversUpTo: 'a1' });
    expect(promptHistory([u1, a1, legacy, u2]).map((m) => m.id)).toEqual(['u1', 'a1', 'u2']);
    expect(promptHistory([u1, a1, u2, marked]).map((m) => m.id)).toEqual(['s1', 'u2']);
  });
});
