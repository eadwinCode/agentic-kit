import { describe, expect, it } from 'bun:test';
import { simulateReadableStream } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { z } from 'zod';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryRunStreams, MemoryStorage } from '../src/adapters/memory.js';
import { openSqlite, SqliteStorage } from '../src/adapters/sqlite.js';
import { agentTool } from '../src/core/tools.js';
import { RECORD_EVENT_TYPES } from '../src/core/publish.js';
import { resolveConfig } from '../src/core/types.js';

// Workstream S5: the thread record keeps only what must outlive a run. The
// Go package runs the same cases under the same names (thread_record_test.go).

function scriptedModel(steps: Array<{ text?: string; call?: { id: string; name: string } }>) {
  let n = 0;
  return new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock',
    doStream: async () => {
      const step = steps[Math.min(n++, steps.length - 1)]!;
      const chunks: LanguageModelV1StreamPart[] = [];
      if (step.text) chunks.push({ type: 'text-delta', textDelta: step.text });
      if (step.call) {
        chunks.push({ type: 'tool-call', toolCallType: 'function', toolCallId: step.call.id, toolName: step.call.name, args: '{}' });
      }
      chunks.push({ type: 'finish', finishReason: step.call ? 'tool-calls' : 'stop', usage: { promptTokens: 10, completionTokens: 5 } });
      return { stream: simulateReadableStream({ chunks }), rawCall: { rawPrompt: null, rawSettings: {} } };
    },
  });
}

async function makeRuntime(model: any) {
  const storage = new MemoryStorage();
  const queue = new MemoryQueue();
  const streams = new MemoryRunStreams();
  const runtime = await setupAgentCore({
    storage, admin: new MemoryAdminStore(), bus: new MemoryBus(), queue, kv: new MemoryKv(), streams,
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
    config: resolveConfig({ streamFlushMs: 0 }),
  });
  return { runtime, storage, queue, streams };
}

describe('thread record', () => {
  it('a run writes no CHUNK to the thread record', async () => {
    const r = await makeRuntime(scriptedModel([{ text: 'looking', call: { id: 'c1', name: 'look' } }, { text: 'done' }]));
    const chat = r.runtime.createStreamTextAgent({
      name: 'chat', model: 'gpt-4o',
      tools: { look: agentTool({ parameters: z.object({}), execute: async () => 'seen' }) },
    });
    const ran = await chat.run({ prompt: 'hi' });
    await r.runtime.worker.handleJob(r.queue.items.shift()!);
    const types = (await r.storage.events.listSince(ran.threadId, -1)).map((e) => e.type);
    expect(types).toEqual(['RUN_STARTED', 'RUN_ENDED']);
    expect(types.every((t) => RECORD_EVENT_TYPES.has(t))).toBe(true);
  });

  it('each segment starts and ends in the record', async () => {
    const r = await makeRuntime(scriptedModel([{ text: 'hi' }]));
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const ran = await chat.run({ prompt: 'hi' });
    await r.runtime.worker.handleJob(r.queue.items.shift()!);
    const [started, ended] = await r.storage.events.list(ran.threadId, { types: ['RUN_STARTED', 'RUN_ENDED'] });
    expect(started).toMatchObject({ runId: ran.runId, payload: { runId: ran.runId, streamId: `${ran.runId}:1`, segment: 1 } });
    expect(ended).toMatchObject({ runId: ran.runId, payload: { streamId: `${ran.runId}:1`, status: 'finished' } });
    expect(ended!.seq).toBeGreaterThan(started!.seq);
  });

  const stores: Array<readonly [string, () => Promise<unknown>]> = [
    ['memory', async () => new MemoryStorage()],
    ['sqlite', async () => new SqliteStorage(await openSqlite(':memory:'))],
  ];
  // Prisma against a real database, through the example app's client.
  if (process.env.TEST_PRISMA_DB) {
    stores.push(['prisma', async () => {
      const { createRequire } = await import('node:module');
      const { join } = await import('node:path');
      const require = createRequire(join(import.meta.dir, '../../../examples/nextjs-app/package.json'));
      const { PrismaClient } = require('@prisma/client');
      const { PrismaStorage } = await import('../src/adapters/prisma.js');
      return new PrismaStorage(new PrismaClient({ datasources: { db: { url: process.env.TEST_PRISMA_DB } } }));
    }]);
  }
  for (const [name, open] of stores) {
    it(`the store mints seqs in order, one per entry (${name})`, async () => {
      const storage: any = await open();
      const th = await storage.threads.create({});
      const seqs = await Promise.all(
        Array.from({ length: 5 }, (_, i) =>
          storage.events.append(th.id, { type: 'CONTEXT_COMPACTED', payload: { i }, runId: i % 2 ? 'r1' : null })),
      );
      expect(seqs.map((e: any) => e.seq).sort()).toEqual([1, 2, 3, 4, 5]);
      expect(await storage.events.list(th.id, { after: 3 })).toHaveLength(2);
      expect(await storage.events.list(th.id, { limit: 2 })).toHaveLength(2);
      expect(await storage.events.list(th.id, { runId: 'r1' })).toHaveLength(2);
      expect(await storage.events.list(th.id, { types: ['INPUT_REQUIRED'] })).toHaveLength(0);
    });
  }

  it('the snapshot carries the latest run stream, not every run', async () => {
    const r = await makeRuntime(scriptedModel([{ text: 'one' }, { text: 'two' }, { text: 'three' }]));
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    let ran: any;
    for (const prompt of ['a', 'b', 'c']) {
      ran = await chat.run({ threadId: ran?.threadId, prompt });
      await r.runtime.worker.handleJob(r.queue.items.shift()!);
    }
    const snap = await r.runtime.getThreadSnapshot(ran.threadId);
    expect(snap!.stream).toMatchObject({ streamId: `${ran.runId}:1`, runId: ran.runId, end: { status: 'finished' } });
    // The finished step is in the messages, so none of its text is in the items.
    expect(snap!.stream!.items.some((i) => i.type === 'TEXT_MESSAGE_CONTENT')).toBe(false);
    expect(snap!.stream!.offset).not.toBeNull();
  });

  it('a durable CUSTOM goes to the thread record; a plain one to the stream', async () => {
    const r = await makeRuntime(scriptedModel([{ call: { id: 'c1', name: 'go' } }, { text: 'done' }]));
    const chat = r.runtime.createStreamTextAgent({
      name: 'chat', model: 'gpt-4o',
      tools: {
        go: agentTool({
          parameters: z.object({}),
          execute: async (_a, { publishEvent }) => {
            await publishEvent('INVOICE_CREATED', { id: 'inv_1' }, { durable: true });
            await publishEvent('PROGRESS', { pct: 50 });
            return 'ok';
          },
        }),
      },
    });
    const ran = await chat.run({ prompt: 'hi' });
    await r.runtime.worker.handleJob(r.queue.items.shift()!);
    const record = (await r.storage.events.listSince(ran.threadId, -1)).map((e) => e.type);
    expect(record).toContain('INVOICE_CREATED');
    expect(record).not.toContain('PROGRESS');
    // Each reaches a tab once: the durable one as its record entry, the
    // plain one on the stream.
    const custom = (await r.streams.snapshot(`${ran.runId}:1`))!.items
      .filter((i) => i.type === 'CUSTOM').map((i: any) => i.name);
    expect(custom).toEqual(['PROGRESS']);
  });
});
