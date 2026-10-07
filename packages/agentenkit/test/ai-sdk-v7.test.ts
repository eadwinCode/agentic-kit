import { describe, expect, it } from 'bun:test';
import { simulateReadableStream, tool } from 'ai';
import { MockLanguageModelV4 } from 'ai/test';
import { z } from 'zod';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { resolveConfig } from '../src/core/types.js';
import { attributeTokens, fillTokens } from '../src/core/usage.js';
import {
  ChunkMapper,
  finishedByProvider,
  thrownByTool,
  toModelMessage,
  toStoredMessage,
} from '../src/core/sdk.js';
import { ofType, runItems } from './stream-helpers.js';

// The platform stores and publishes the v4 shape, shared with the Go runtime;
// the SDK (v7) speaks its own. These cases pin the translation between them,
// and run one turn against the SDK's own v4 fake model, with no shim between.

describe('stored messages ↔ SDK messages', () => {
  it('turns a stored tool call and result into the SDK shape', () => {
    expect(toModelMessage({
      role: 'assistant',
      content: [{ type: 'tool-call', toolCallId: 'c1', toolName: 'lookup', args: { q: 'x' } }],
    })).toEqual({
      role: 'assistant',
      content: [{ type: 'tool-call', toolCallId: 'c1', toolName: 'lookup', input: { q: 'x' } }],
    });
    expect(toModelMessage({
      role: 'tool',
      content: [
        { type: 'tool-result', toolCallId: 'c1', toolName: 'lookup', result: { found: true } },
        { type: 'tool-result', toolCallId: 'c2', toolName: 'echo', result: 'plain' },
      ],
    }).content).toEqual([
      { type: 'tool-result', toolCallId: 'c1', toolName: 'lookup', output: { type: 'json', value: { found: true } } },
      { type: 'tool-result', toolCallId: 'c2', toolName: 'echo', output: { type: 'text', value: 'plain' } },
    ]);
  });

  it('parses arguments stored as JSON text', () => {
    const m = toModelMessage({
      role: 'assistant',
      content: [{ type: 'tool-call', toolCallId: 'c1', toolName: 'lookup', args: '{"q":"x"}' }],
    });
    expect((m.content as any[])[0].input).toEqual({ q: 'x' });
  });

  it('renames an image mimeType and moves a reasoning signature', () => {
    const m = toModelMessage({
      role: 'user',
      content: [{ type: 'image', image: 'https://cdn.example/cat.png', mimeType: 'image/png' }],
    });
    expect((m.content as any[])[0]).toEqual({ type: 'image', image: 'https://cdn.example/cat.png', mediaType: 'image/png' });
    const r = toModelMessage({ role: 'assistant', content: [{ type: 'reasoning', text: 'hm', signature: 'sig' }] });
    expect((r.content as any[])[0]).toEqual({
      type: 'reasoning', text: 'hm', providerOptions: { anthropic: { signature: 'sig' } },
    });
  });

  it('reads the v4 provider-metadata name as provider options', () => {
    const stamp = { anthropic: { cacheControl: { type: 'ephemeral' } } };
    const m = toModelMessage({ role: 'system', content: 'hi', experimental_providerMetadata: stamp });
    expect(m).toEqual({ role: 'system', content: 'hi', providerOptions: stamp });
  });

  it('turns what the SDK produced back into the stored shape', () => {
    expect(toStoredMessage({
      role: 'tool',
      content: [{ type: 'tool-result', toolCallId: 'c1', toolName: 'lookup', output: { type: 'json', value: [1, 2] } }],
    }).content).toEqual([{ type: 'tool-result', toolCallId: 'c1', toolName: 'lookup', result: [1, 2] }]);
    expect(toStoredMessage({
      role: 'assistant',
      content: [{ type: 'tool-call', toolCallId: 'c1', toolName: 'lookup', input: { q: 'x' } }],
    }).content).toEqual([{ type: 'tool-call', toolCallId: 'c1', toolName: 'lookup', args: { q: 'x' } }]);
  });
});

describe('SDK stream parts → published chunks', () => {
  it('maps each part to the shape core/stream.go publishes', () => {
    const m = new ChunkMapper();
    expect(m.map({ type: 'text-start', id: 't' })).toBeNull();
    expect(m.map({ type: 'text-delta', id: 't', text: 'Hi' })).toEqual({ type: 'text-delta', textDelta: 'Hi' });
    expect(m.map({ type: 'reasoning-delta', id: 'r', text: 'hm' })).toEqual({ type: 'reasoning', textDelta: 'hm' });
    expect(m.map({ type: 'tool-input-start', id: 'c1', toolName: 'search' })).toEqual({
      type: 'tool-call-streaming-start', toolCallId: 'c1', toolName: 'search',
    });
    expect(m.map({ type: 'tool-input-delta', id: 'c1', delta: '{"q":' })).toEqual({
      type: 'tool-call-delta', toolCallId: 'c1', toolName: 'search', argsTextDelta: '{"q":',
    });
    expect(m.map({ type: 'tool-call', toolCallId: 'c1', toolName: 'search', input: { q: 'x' } })).toEqual({
      type: 'tool-call', toolCallId: 'c1', toolName: 'search', args: { q: 'x' },
    });
    expect(m.map({ type: 'tool-result', toolCallId: 'c1', toolName: 'search', input: {}, output: ['a'] })).toEqual({
      type: 'tool-result', toolCallId: 'c1', toolName: 'search', result: ['a'],
    });
    expect(m.map({ type: 'source', sourceType: 'url', id: 's', url: 'https://example.com' })).toEqual({
      type: 'source', source: { sourceType: 'url', id: 's', url: 'https://example.com' },
    });
    expect(m.map({
      type: 'finish-step',
      finishReason: 'stop',
      usage: { inputTokens: 10, outputTokens: 5, totalTokens: 15, inputTokenDetails: { cacheReadTokens: 2 } },
    })).toEqual({
      type: 'step-finish', finishReason: 'stop',
      usage: { promptTokens: 10, completionTokens: 5, totalTokens: 15, cachedInputTokens: 2 },
    });
  });

  it('publishes a bad call as a failed result, and a stop not at all', () => {
    const m = new ChunkMapper();
    const { NoSuchToolError } = require('ai');
    expect(m.map({
      type: 'tool-error', toolCallId: 'c1', toolName: 'nope', input: {},
      error: new NoSuchToolError({ toolName: 'nope' }),
    })).toMatchObject({ type: 'tool-result', toolCallId: 'c1', result: { error: expect.any(String) } });
    expect(m.map({ type: 'tool-error', toolCallId: 'c2', toolName: 'x', input: {}, error: new Error('stopped') })).toBeNull();
  });

  // A tool the provider ran itself failed on the provider's side: nothing of
  // ours threw, so the model reads the failure and the step goes on.
  it('publishes a provider-run tool failure, and does not end the step on it', () => {
    const part = {
      type: 'tool-error', toolCallId: 'c3', toolName: 'web_search', input: {},
      error: new Error('search failed'), providerExecuted: true,
    };
    expect(new ChunkMapper().map(part)).toEqual({
      type: 'tool-result', toolCallId: 'c3', toolName: 'web_search', result: { error: 'search failed' },
    });
    expect(thrownByTool([part])).toBeUndefined();
    const ours = { ...part, providerExecuted: undefined };
    expect(thrownByTool([ours])).toBe(part.error);
  });
});

describe('finish and usage', () => {
  it('a finish with no reason is not a finish', () => {
    expect(finishedByProvider('stop', 'stop')).toBe(true);
    expect(finishedByProvider('other', 'weird')).toBe(true);
    expect(finishedByProvider('other', undefined)).toBe(false);
  });

  // v7 reports cache reads in usage, counted inside inputTokens. The provider
  // metadata may still carry them too; reading both would count one hit twice.
  it('reads the cache split from usage, and not again from metadata', () => {
    const usage = {
      inputTokens: 1200,
      inputTokenDetails: { noCacheTokens: 176, cacheReadTokens: 1024, cacheWriteTokens: 30 },
      outputTokens: 50,
      outputTokenDetails: { textTokens: 40, reasoningTokens: 10 },
      totalTokens: 1250,
    };
    const meta = { openai: { cachedPromptTokens: 1024 } };
    expect(attributeTokens(usage, meta)).toEqual({
      inputTokens: 176, cachedInputTokens: 1024, outputTokens: 50, totalTokens: 1250,
    });
    const filled = fillTokens(usage, meta);
    expect(filled.cacheWriteInputTokens).toBe(30);
    expect(filled.reasoningTokens).toBe(10);
  });

  it('leaves cache writes out of fresh input when usage has no uncached count', () => {
    const usage = {
      inputTokens: 1000,
      inputTokenDetails: { noCacheTokens: undefined, cacheReadTokens: 200, cacheWriteTokens: 300 },
      outputTokens: 10,
      totalTokens: 1010,
    };
    const filled = fillTokens(usage);
    expect(filled.inputTokens).toBe(500);
    expect(filled.cacheReadInputTokens).toBe(200);
    expect(filled.cacheWriteInputTokens).toBe(300);
  });

  it('falls back to metadata when usage has no split', () => {
    const usage = {
      inputTokens: 1200,
      inputTokenDetails: { noCacheTokens: undefined, cacheReadTokens: undefined, cacheWriteTokens: undefined },
      outputTokens: 50,
      totalTokens: 1250,
    };
    expect(attributeTokens(usage, { openai: { cachedPromptTokens: 1024 } }).cachedInputTokens).toBe(1024);
  });
});

describe('one turn on the SDK’s own fake model', () => {
  it('stores and publishes the platform shape', async () => {
    const usage = (input: number, cacheRead: number, output: number) => ({
      inputTokens: { total: input, noCache: input - cacheRead, cacheRead, cacheWrite: undefined },
      outputTokens: { total: output, text: output, reasoning: undefined },
    });
    const prompts: any[] = [];
    let call = 0;
    const model = new MockLanguageModelV4({
      doStream: async (options: any) => {
        prompts.push(options.prompt);
        const chunks: any[] = call++ === 0
          ? [
              { type: 'tool-input-start', id: 'c1', toolName: 'lookup' },
              { type: 'tool-input-delta', id: 'c1', delta: '{"q":"x"}' },
              { type: 'tool-input-end', id: 'c1' },
              { type: 'tool-call', toolCallId: 'c1', toolName: 'lookup', input: '{"q":"x"}' },
              { type: 'finish', finishReason: { unified: 'tool-calls', raw: 'tool_calls' }, usage: usage(100, 0, 10) },
            ]
          : [
              { type: 'text-start', id: 't' },
              { type: 'text-delta', id: 't', delta: 'found ' },
              { type: 'text-delta', id: 't', delta: 'it' },
              { type: 'text-end', id: 't' },
              { type: 'finish', finishReason: { unified: 'stop', raw: 'stop' }, usage: usage(200, 64, 5) },
            ];
        return { stream: simulateReadableStream({ chunks }) };
      },
    });
    const storage = new MemoryStorage();
    const bus = new MemoryBus();
    const queue = new MemoryQueue();
    const runtime = await setupAgentCore({
      storage, bus, queue, kv: new MemoryKv(), admin: new MemoryAdminStore(),
      config: resolveConfig(),
      resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
    });
    const lookup = tool({ inputSchema: z.object({ q: z.string() }), execute: async ({ q }) => ({ found: q }) });
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'm', system: 'be brief', tools: { lookup } });
    const ran = await chat.run({ prompt: 'find x' });
    await runtime.worker.handleJob(queue.items.shift()!);

    // Stored in the platform shape.
    const stored = storage.messages.store.get(ran.threadId)!;
    expect(stored.find((m) => m.role === 'assistant' && Array.isArray(m.content))!.content).toContainEqual(
      expect.objectContaining({ type: 'tool-call', toolCallId: 'c1', toolName: 'lookup', args: { q: 'x' } }),
    );
    expect(stored.find((m) => m.role === 'tool')!.content).toEqual([
      expect.objectContaining({ type: 'tool-result', toolCallId: 'c1', toolName: 'lookup', result: { found: 'x' } }),
    ]);

    // The second step's prompt went to the model in the SDK shape.
    const toolTurn = prompts[1].find((m: any) => m.role === 'tool');
    expect(toolTurn.content[0]).toMatchObject({ type: 'tool-result', output: { type: 'json', value: { found: 'x' } } });
    expect(prompts[0][0]).toMatchObject({ role: 'system', content: 'be brief' });

    // Published in the platform shape, as the stream events a client reads.
    const items = await runItems(runtime.ports(), ran.runId!);
    expect(ofType(items, 'TOOL_CALL_START')).toContainEqual(expect.objectContaining({ toolCallId: 'c1', toolName: 'lookup' }));
    expect(ofType(items, 'TOOL_CALL_ARGS').map((e) => e.delta).join('')).toBe('{"q":"x"}');
    expect(ofType(items, 'TOOL_CALL_END')).toContainEqual(expect.objectContaining({ toolCallId: 'c1', args: { q: 'x' } }));
    expect(ofType(items, 'TOOL_CALL_RESULT')).toContainEqual(expect.objectContaining({ toolCallId: 'c1', result: { found: 'x' } }));
    expect(ofType(items, 'TEXT_MESSAGE_CONTENT').map((e) => e.delta).join('')).toBe('found it');

    // Billed from the usage split: the cache hit is not counted as fresh input.
    const rows = storage.usage.recorded.filter((u) => u.threadId === ran.threadId);
    expect(rows.map((u) => [u.inputTokens, u.cacheReadInputTokens, u.outputTokens])).toEqual([
      [100, 0, 10],
      [136, 64, 5],
    ]);
  });
});
