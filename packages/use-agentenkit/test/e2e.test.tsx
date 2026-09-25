import { afterAll, afterEach, beforeAll, describe, expect, it } from 'bun:test';
import { act, cleanup, renderHook, waitFor } from '@testing-library/react';
import * as web from 'node:stream/web';
import { useAgentThread } from '../src/useAgentThread.js';
import type { AgentRunConfig } from '../src/config.js';
// The real runtime, in process: the hook reads exactly the frames a server
// sends, not hand-made ones.
import { setupAgentCore } from '../../agentenkit/src/runtime.js';
import { MemoryAdminStore } from '../../agentenkit/src/admin/memory.js';
import {
  MemoryBus, MemoryKv, MemoryQueue, MemoryRunStreams, MemoryStorage,
} from '../../agentenkit/src/adapters/memory.js';
import { resolveConfig } from '../../agentenkit/src/core/types.js';

// Workstream S7, end to end: a run's text reaches the hook through the run
// stream, and each answer shows once.

afterEach(() => cleanup());

// The test's DOM swaps the global streams for copies the AI SDK's transforms
// do not accept. The runtime runs here too, so it gets Node's own.
const domStreams = {
  ReadableStream: globalThis.ReadableStream,
  WritableStream: globalThis.WritableStream,
  TransformStream: globalThis.TransformStream,
};
beforeAll(() => {
  Object.assign(globalThis, {
    ReadableStream: web.ReadableStream, WritableStream: web.WritableStream, TransformStream: web.TransformStream,
  });
});
afterAll(() => {
  Object.assign(globalThis, domStreams);
});

/** A model that says each reply in the given pieces, one call per reply. */
function model(...replies: string[][]) {
  let call = 0;
  const parts = () => {
    const pieces = replies[Math.min(call++, replies.length - 1)]!;
    return [
      ...pieces.map((textDelta) => ({ type: 'text-delta', textDelta })),
      { type: 'finish', finishReason: 'stop', usage: { promptTokens: 10, completionTokens: 5 } },
    ];
  };
  return {
    specificationVersion: 'v1', provider: 'mock', modelId: 'mock', defaultObjectGenerationMode: undefined,
    doStream: async () => ({
      stream: new ReadableStream({
        start(controller) {
          for (const p of parts()) controller.enqueue(p);
          controller.close();
        },
      }),
      rawCall: { rawPrompt: null, rawSettings: {} },
    }),
    doGenerate: async () => { throw new Error('not used'); },
  };
}

async function server(m: any) {
  const queue = new MemoryQueue();
  const runtime = await setupAgentCore({
    storage: new MemoryStorage(), admin: new MemoryAdminStore(), bus: new MemoryBus(), queue, kv: new MemoryKv(),
    streams: new MemoryRunStreams(),
    resolveModel: () => ({ instance: () => m, contextWindow: 128_000 }),
    config: resolveConfig({ streamFlushMs: 0 }),
  });
  const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
  const json = (body: unknown, status = 200) =>
    new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } });

  const config: AgentRunConfig = {
    loadThreadsOnMount: false,
    threadsRefreshMs: false,
    persistence: false,
    fetch: async (input, init) => {
      const url = new URL(String(input), 'http://localhost');
      if (url.pathname.endsWith('/history')) {
        const snap = await runtime.getThreadSnapshot(url.searchParams.get('threadId')!);
        return snap ? json(snap) : json({ error: 'Thread not found' }, 404);
      }
      if (url.pathname.endsWith('/run')) {
        const body = JSON.parse(String(init?.body));
        return json(await chat.run({ threadId: body.threadId, prompt: body.prompt }));
      }
      if (url.pathname.endsWith('/usage')) return json(null);
      return json({ ok: true });
    },
    // The route handler, minus the HTTP: the follow's frames, as SSE data.
    openStream: (url, handlers) => {
      const q = new URL(url, 'http://localhost').searchParams;
      const abort = new AbortController();
      void (async () => {
        const frames = runtime.events.follow(q.get('threadId')!, {
          cursor: q.get('cursor'), lastMessageId: q.get('lastMessageId') ?? undefined, signal: abort.signal,
        });
        for await (const frame of frames) handlers.onMessage(JSON.stringify(frame));
      })();
      return { close: () => abort.abort() };
    },
  };
  const work = async () => {
    while (queue.items.length > 0) await runtime.worker.handleJob(queue.items.shift()!);
  };
  return { runtime, chat, config, work };
}

describe('run streams, end to end', () => {
  it('a run streams its text into the hook and ends completed', async () => {
    const s = await server(model(['Hel', 'lo']));
    const thread = await s.runtime.ports().storage.threads.create({});
    const view = renderHook(() => useAgentThread({ initialThreadId: thread.id, ...s.config }));
    await waitFor(() => expect(view.result.current.historyLoading).toBe(false));
    await act(async () => {
      await view.result.current.run('hi');
    });
    await act(async () => {
      await s.work();
    });
    await waitFor(() => {
      expect(view.result.current.agentState).toBe('COMPLETED');
      expect(view.result.current.entries.at(-1)!.text).toBe('Hello');
    });
  });

  it('a tab open across two runs shows each answer once', async () => {
    const s = await server(model(['one '], ['two']));
    // Run 1 is done before the tab opens, so it comes from the snapshot; run
    // 2 comes live, from the next run's stream.
    const thread = await s.runtime.ports().storage.threads.create({});
    await s.chat.run({ threadId: thread.id, prompt: 'a' });
    await s.work();
    const view = renderHook(() => useAgentThread({ initialThreadId: thread.id, ...s.config }));
    await waitFor(() => expect(view.result.current.historyLoading).toBe(false));
    await waitFor(() => expect(view.result.current.entries.at(-1)!.text).toBe('one '));
    await act(async () => {
      await s.chat.run({ threadId: thread.id, prompt: 'b' });
      await s.work();
    });
    await waitFor(() => {
      const texts = view.result.current.entries.filter((e) => e.role === 'assistant').map((e) => e.text);
      expect(texts).toEqual(['one ', 'two']); // each once
    });
  });
});
