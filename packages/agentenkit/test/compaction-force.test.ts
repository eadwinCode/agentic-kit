import { describe, expect, it } from 'bun:test';
import { simulateReadableStream } from 'ai';
import type { LanguageModelV1StreamPart } from './v1-mock.js';
import { MockLanguageModelV1 } from './v1-mock.js';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { chunkLines, isContextOverflow } from '../src/core/context.js';
import { resolveConfig, type AgentConfig } from '../src/core/types.js';
import type { RunFinishInfo } from '../src/ports/runtime.js';

// Recovering from a prompt the provider refused as too long, compactThread,
// summarizing a history larger than the summarizer in parts, and a late
// settle that sees the run's state. The same cases run in the Go package
// (compaction_force_test.go, compaction_chunks_test.go,
// core/summary_chunks_test.go, settle_claim_test.go).

/** Answers every streamed call "ok" and every summary request with `summary`,
 *  and keeps the prompts it was sent. `refuse` refuses one streamed call as
 *  too long. */
function model(opts: { summary?: string; refuse?: (prompt: string) => boolean } = {}) {
  const streamed: string[] = [];
  const summarized: string[] = [];
  let refused = false;
  const m = new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock-summarizing',
    doGenerate: async ({ prompt }: any) => {
      summarized.push(JSON.stringify(prompt));
      return {
        text: opts.summary ?? 'summary',
        finishReason: 'stop',
        usage: { promptTokens: 100, completionTokens: 10 },
        rawCall: { rawPrompt: null, rawSettings: {} },
      };
    },
    doStream: async ({ prompt }: any) => {
      const text = JSON.stringify(prompt);
      if (!refused && opts.refuse?.(text)) {
        refused = true;
        throw new Error('prompt is too long: 224705 tokens > 200000 maximum');
      }
      streamed.push(text);
      const chunks: LanguageModelV1StreamPart[] = [
        { type: 'text-delta', textDelta: 'ok' },
        { type: 'finish', finishReason: 'stop', usage: { promptTokens: 10, completionTokens: 5 } },
      ];
      return { stream: simulateReadableStream({ chunks }), rawCall: { rawPrompt: null, rawSettings: {} } };
    },
  });
  return { m, streamed, summarized, refused: () => refused };
}

async function harness(
  models: Record<string, { m: MockLanguageModelV1; window?: number }>,
  config: Partial<AgentConfig>,
) {
  const storage = new MemoryStorage();
  const queue = new MemoryQueue();
  const runtime = await setupAgentCore({
    storage, queue, bus: new MemoryBus(), kv: new MemoryKv(), admin: new MemoryAdminStore(),
    config: resolveConfig({ promptCaching: false, ...config }),
    resolveModel: (key: string) => {
      const entry = models[key] ?? models['gpt-4o']!;
      return { instance: () => entry.m, contextWindow: entry.window ?? 128_000 };
    },
  });
  const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
  const runPrompt = async (threadId: string | undefined, i: number) => {
    const ran = await chat.run({ ...(threadId ? { threadId } : {}), prompt: `run-${i} ${'x'.repeat(1_200)}` });
    await runtime.worker.handleJob(queue.items.shift()!);
    return ran;
  };
  const summaries = (threadId: string) =>
    storage.messages.store.get(threadId)!.filter((msg) => (msg.content as any)?.type === 'CONTEXT_SUMMARY').length;
  return { runtime, storage, queue, runPrompt, summaries };
}

describe('forced compaction (§2.6)', () => {
  it('compactThread forces a summary', async () => {
    const chat = model();
    const h = await harness({ 'gpt-4o': chat }, {
      contextCeilingTokens: 2_000, contextOutputReserveTokens: 200, compactionModel: 'gpt-4o',
    });
    const first = await h.runPrompt(undefined, 1);
    await h.runPrompt(first.threadId, 2);
    await h.runPrompt(first.threadId, 3);
    expect(h.summaries(first.threadId)).toBe(0); // under the trigger
    expect(await h.runtime.compactThread(first.threadId)).toEqual({ compacted: true });
    expect(h.summaries(first.threadId)).toBe(1);
    const missing = await h.runtime.compactThread('missing');
    expect(missing.compacted).toBe(false);
    expect(missing.reason).toBeTruthy();
  });

  it('a prompt too long compacts and retries', async () => {
    const chat = model({ refuse: (p) => p.includes('run-3 ') });
    const h = await harness({ 'gpt-4o': chat }, {
      contextCeilingTokens: 100_000, // far from the trigger: only the refusal compacts
      compactionModel: 'gpt-4o',
    });
    const first = await h.runPrompt(undefined, 1);
    await h.runPrompt(first.threadId, 2);
    await h.runPrompt(first.threadId, 3);
    expect(chat.refused()).toBe(true);
    expect(h.summaries(first.threadId)).toBe(1);
    const last = chat.streamed.at(-1)!;
    expect(last).toContain('run-3 '); // the retry sends the summary and the new turn
    expect(last).not.toContain('run-1 ');
    expect((await h.storage.threads.get(first.threadId))!.state).toBe('COMPLETED');
  });

  it('isContextOverflow', () => {
    for (const message of [
      'prompt is too long: 224705 tokens > 200000 maximum',
      "This model's maximum context length is 128000 tokens",
      'context_length_exceeded',
    ]) {
      expect(isContextOverflow(new Error(message))).toBe(true);
    }
    expect(isContextOverflow(new Error('rate limited'))).toBe(false);
    expect(isContextOverflow(null)).toBe(false);
    // A throttle is not an overflow, however it is worded.
    expect(isContextOverflow(new Error('ThrottlingException: Too many tokens, please wait before trying again.'))).toBe(false);
    expect(isContextOverflow(Object.assign(new Error('prompt is too long'), { statusCode: 429 }))).toBe(false);
  });

  it('a history larger than the summarizer is chunked', async () => {
    const chat = model();
    const tiny = model({ summary: 'part summary' });
    const h = await harness({ 'gpt-4o': chat, tiny: { m: tiny.m, window: 2_000 } }, {
      contextCeilingTokens: 100_000, // no automatic compaction
      compactionModel: 'tiny',
    });
    const first = await h.runPrompt(undefined, 1);
    for (let i = 2; i <= 8; i++) await h.runPrompt(first.threadId, i);
    expect(await h.runtime.compactThread(first.threadId)).toEqual({ compacted: true });
    expect(tiny.summarized.length).toBeGreaterThanOrEqual(3); // parts plus a merge
    for (const p of tiny.summarized) expect(p.length).toBeLessThanOrEqual(1_000 * 4 + 1_000);
    expect(tiny.summarized.at(-1)).toContain('Merge them');
    expect(h.summaries(first.threadId)).toBe(1);
  });
});

describe('chunkLines', () => {
  it('packs in order under the limit', () => {
    const chunks = chunkLines(['a'.repeat(400), 'b'.repeat(400), 'c'.repeat(400)], 250); // two fit, the third does not
    expect(chunks).toHaveLength(2);
    expect(chunks[0]!.startsWith('a')).toBe(true);
    expect(chunks[1]!.startsWith('c')).toBe(true);
    expect(chunkLines([], 250)).toEqual(['']);
  });

  it('a line larger than a chunk keeps its ends', () => {
    const [got, ...rest] = chunkLines([`START${'m'.repeat(10_000)}END`], 500);
    expect(rest).toHaveLength(0);
    expect(got!.startsWith('START')).toBe(true);
    expect(got!.endsWith('END')).toBe(true);
    expect(got).toContain('[... cut ...]');
    expect(Math.ceil(got!.length / 4)).toBeLessThanOrEqual(520);
  });
});

describe('late settle (§5.6)', () => {
  it("the sweep gives the hook the run's state", async () => {
    const seen: unknown[] = [];
    const chat = model();
    const storage = new MemoryStorage();
    const queue = new MemoryQueue();
    const runtime = await setupAgentCore({
      storage, queue, bus: new MemoryBus(), kv: new MemoryKv(), admin: new MemoryAdminStore(),
      config: resolveConfig({ promptCaching: false }),
      resolveModel: () => ({ instance: () => chat.m, contextWindow: 128_000 }),
    });
    const agent = runtime.createStreamTextAgent({
      name: 'chat',
      onSettle: (info: RunFinishInfo) => {
        seen.push(info.runState?.teamId);
        if (seen.length === 1) throw new Error('ledger down');
      },
    });
    await agent.run({ prompt: 'go', state: { teamId: 'team-1' } });
    await runtime.worker.handleJob(queue.items.shift()!);
    const report = await runtime.reclaimStuckRuns(0);
    expect(report.settled).toBe(1);
    expect(seen).toEqual(['team-1', 'team-1']);
  });
});
