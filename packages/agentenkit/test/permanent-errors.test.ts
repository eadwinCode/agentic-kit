import { describe, expect, it } from 'bun:test';
import { APICallError } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { isPermanentError } from '../src/core/permanent.js';
import { resolveConfig } from '../src/core/types.js';

// A run that fails with an error retrying cannot fix is failed at once. The
// same cases run in the Go package (permanent_errors_test.go).

const apiError = (statusCode: number, responseBody = '{}') =>
  new APICallError({
    message: `provider answered ${statusCode}`,
    url: 'https://api.example/v1/chat',
    requestBodyValues: {},
    statusCode,
    responseBody,
    // Not retried inside the SDK either, so each case is one model call.
    isRetryable: false,
  });

async function harness(error: Error) {
  let calls = 0;
  const model = new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock-failing',
    doStream: async () => {
      calls++;
      throw error;
    },
  });
  const storage = new MemoryStorage();
  const bus = new MemoryBus();
  const queue = new MemoryQueue();
  const runtime = await setupAgentCore({
    storage, bus, queue, kv: new MemoryKv(), admin: new MemoryAdminStore(),
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
    config: resolveConfig({ stopPollMs: 5, runRetryBackoffMs: 0, promptCaching: false }),
  });
  const chat = runtime.createStreamTextAgent({ name: 'chat' });
  const ran = await chat.run({ prompt: 'hi' });
  await runtime.worker.handleJob(queue.items.shift()!);
  return {
    calls: () => calls,
    state: async () => (await storage.threads.get(ran.threadId))!.state,
    queued: () => queue.items.length,
    lastError: () =>
      bus.published
        .filter((e) => e.type === 'STATE_CHANGE' && (e.payload as any).state === 'FAILED')
        .map((e) => (e.payload as any).error)
        .at(-1),
  };
}

describe('errors retrying cannot fix (§2.8)', () => {
  it('an error that retrying cannot fix fails the run at once', async () => {
    for (const status of [400, 401, 403, 404, 413, 422]) {
      const h = await harness(apiError(status));
      expect({ status, state: await h.state(), queued: h.queued(), calls: h.calls() })
        .toEqual({ status, state: 'FAILED', queued: 0, calls: 1 });
      expect(h.lastError()).toContain(`provider answered ${status}`);
    }
  });

  it('no credits fails the run at once, though it comes as a 429', async () => {
    const h = await harness(apiError(429, '{"error":{"code":"insufficient_quota","message":"You exceeded your current quota"}}'));
    expect(await h.state()).toBe('FAILED');
    expect(h.queued()).toBe(0);
  });

  it('a rate limit is still retried', async () => {
    const h = await harness(apiError(429, '{"error":{"code":"rate_limit_exceeded"}}'));
    expect(await h.state()).toBe('QUEUED');
    expect(h.queued()).toBe(1); // the retry
  });

  it('a server error is still retried', async () => {
    const h = await harness(apiError(503));
    expect(await h.state()).toBe('QUEUED');
    expect(h.queued()).toBe(1);
  });

  it('isPermanentError looks inside the errors that wrap it', () => {
    const wrapped = Object.assign(new Error('Failed after 3 attempts'), { lastError: apiError(401) });
    expect(isPermanentError(wrapped)).toBe(true);
    expect(isPermanentError(new Error('outer', { cause: apiError(404) }))).toBe(true);
    expect(isPermanentError(new Error('network down'))).toBe(false);
    expect(isPermanentError(undefined)).toBe(false);
  });
});
