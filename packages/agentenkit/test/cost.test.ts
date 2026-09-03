import { describe, expect, it } from 'bun:test';
import { tool, simulateReadableStream } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { z } from 'zod';
import { MockLanguageModelV1 } from 'ai/test';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { resolveConfig, type AgentConfig } from '../src/core/types.js';
import type { Pricer, RunFinishInfo, RuntimeOptions } from '../src/ports/runtime.js';
import * as pricing from '../src/pricing.js';

/** $10 per million input, $30 per million output. A step of 10 input + 5
 *  output is therefore 100 + 150 = 250 micros, a quarter of a cent. */
const priceList = pricing.table({ 'gpt-4o': { inputPerMillion: 10, outputPerMillion: 30 } });

interface ScriptedStep {
  text?: string;
  toolCalls?: Array<{ toolCallId: string; toolName: string; args: any }>;
}

function scriptedModel(steps: ScriptedStep[]) {
  let call = 0;
  return new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock-scripted',
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
      return {
        stream: simulateReadableStream({ chunks }),
        rawCall: { rawPrompt: null, rawSettings: {} },
      };
    },
    // Compaction goes through generateText, not the stream (§2.6).
    doGenerate: async () => ({
      text: 'a dense summary',
      finishReason: 'stop' as const,
      usage: { promptTokens: 10, completionTokens: 5 },
      rawCall: { rawPrompt: null, rawSettings: {} },
    }),
  });
}

async function makeRuntime(
  steps: ScriptedStep[],
  opts: { pricer?: Pricer; config?: Partial<AgentConfig> } = {},
) {
  const storage = new MemoryStorage();
  const bus = new MemoryBus();
  const queue = new MemoryQueue();
  const kv = new MemoryKv();
  const model = scriptedModel(steps);
  const resolved: string[] = [];
  const deps: RuntimeOptions = {
    storage,
    admin: new MemoryAdminStore(),
    bus,
    queue,
    kv,
    // The wire id a key resolves to, recorded on every usage row (§4). A
    // resolver that declares none falls back to the key.
    resolveModel: (name: string) => {
      resolved.push(name);
      // Like the real thing, an unknown registry key throws (§3.3).
      if (name.startsWith('unknown-')) throw new Error(`Unknown model: ${name}`);
      return {
        instance: () => model,
        contextWindow: 128_000,
        ...(name === 'gpt-4o' ? { modelId: 'gpt-4o-2024-11-20' } : {}),
      };
    },
    ...(opts.pricer ? { pricer: opts.pricer } : {}),
    config: resolveConfig(opts.config ?? {}),
  };
  return { runtime: await setupAgentCore(deps), storage, bus, queue, resolved };
}

const probe = {
  probe: tool({
    description: 'probe',
    parameters: z.object({}),
    execute: async () => ({ ok: true }),
  }),
};

const events = (bus: MemoryBus, type: string) =>
  bus.published.filter((e) => e.type === type);
const terminal = (bus: MemoryBus) =>
  bus.published.filter((e) => e.type === 'STATE_CHANGE').at(-1)!.payload as any;

describe('cost as part of the usage store (§4)', () => {
  it('prices every call on the row that stores it', async () => {
    const { runtime, storage, queue } = await makeRuntime(
      [{ toolCalls: [{ toolCallId: 'c1', toolName: 'probe', args: {} }] }, { text: 'done' }],
      { pricer: priceList },
    );
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o', tools: probe });
    const ran = await chat.run({ prompt: 'hi' });
    await runtime.worker.handleJob(queue.items[0]!);

    const rows = storage.usage.recorded;
    expect(rows.length).toBe(2);
    for (const r of rows) {
      expect(r.cost).toEqual({ micros: 250, currency: 'USD', source: 'table' });
      expect(r.model).toBe('gpt-4o');
      expect(r.modelId).toBe('gpt-4o-2024-11-20');
    }

    // The read side sums money as well as tokens.
    const usage = (await runtime.getThreadUsage(ran.threadId))!;
    expect(usage.tokens.costMicros).toBe(500);
    expect(usage.tokens.currency).toBe('USD');
    expect(usage.tokens.unpriced).toBe(0);
    expect(usage.tokens.totalTokens).toBe(30);

    // And so does the admin run view, from the same rows.
    const detail = (await runtime.admin.getRun(ran.runId!))!;
    expect(detail.usage.costMicros).toBe(500);
    expect(detail.usage.lines.length).toBe(1);
    expect(detail.usage.lines[0]).toMatchObject({ agentName: 'chat', calls: 2, costMicros: 500 });
  });

  it('stores rows unpriced when no pricer is configured', async () => {
    const { runtime, storage, queue } = await makeRuntime([{ text: 'done' }]);
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const ran = await chat.run({ prompt: 'hi' });
    await runtime.worker.handleJob(queue.items[0]!);

    expect(storage.usage.recorded[0]!.cost).toBeFalsy();
    const usage = (await runtime.getThreadUsage(ran.threadId))!;
    expect(usage.tokens.costMicros).toBe(0);
    // `unpriced` above zero is how a reader tells "spent nothing" apart from
    // "nobody priced it".
    expect(usage.tokens.unpriced).toBe(1);
  });

  it('leaves a model the table does not know unpriced, not free', async () => {
    const { runtime, storage, queue } = await makeRuntime([{ text: 'done' }], { pricer: priceList });
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'some-other-model' });
    const ran = await chat.run({ prompt: 'hi' });
    await runtime.worker.handleJob(queue.items[0]!);

    const usage = (await runtime.getThreadUsage(ran.threadId))!;
    expect(usage.tokens.unpriced).toBe(1);
    expect(usage.tokens.costMicros).toBe(0);
    // This resolver declares no wire id for the key, so the key is the id.
    expect(storage.usage.recorded[0]!.modelId).toBe('some-other-model');
  });

  it('hands onFinish the whole run bill, grouped into lines', async () => {
    let got: RunFinishInfo | undefined;
    const { runtime, queue } = await makeRuntime(
      [{ toolCalls: [{ toolCallId: 'c1', toolName: 'probe', args: {} }] }, { text: 'done' }],
      { pricer: priceList },
    );
    const chat = runtime.createStreamTextAgent({
      name: 'chat', model: 'gpt-4o', tools: probe,
      onFinish: (info) => { got = info; },
    });
    await chat.run({ prompt: 'hi' });
    await runtime.worker.handleJob(queue.items[0]!);

    expect(got?.usage.costMicros).toBe(500);
    expect(got?.usage.currency).toBe('USD');
    expect(got?.usage.unpriced).toBe(0);
    // The bill is the lines: one per agent and model, ready to charge.
    expect(got?.usage.lines).toEqual([
      {
        agentId: null, agentName: 'chat', model: 'gpt-4o', modelId: 'gpt-4o-2024-11-20',
        inputTokens: 20, cacheReadInputTokens: 0, cacheWriteInputTokens: 0,
        outputTokens: 10, reasoningTokens: 0, calls: 2, estimated: 0, costMicros: 500,
      },
    ]);
  });

  it('stops the run between steps when the money runs out', async () => {
    const { runtime, bus, queue } = await makeRuntime(
      [
        { toolCalls: [{ toolCallId: 'c1', toolName: 'probe', args: {} }] },
        { toolCalls: [{ toolCallId: 'c2', toolName: 'probe', args: {} }] },
        { text: 'never reached' },
      ],
      { pricer: priceList },
    );
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o', tools: probe });
    // 400 micros: the first step (250) stays under, the second crosses it.
    const ran = await chat.run({ prompt: 'hi', costBudgetMicros: 400 });
    expect(queue.items[0]!.costBudgetMicros).toBe(400);
    await runtime.worker.handleJob(queue.items[0]!);

    const exhausted = events(bus, 'COST_BUDGET_EXHAUSTED');
    expect(exhausted.length).toBe(1);
    expect(exhausted[0]!.payload).toMatchObject({
      costMicros: 500, costBudgetMicros: 400, currency: 'USD',
    });
    // A money break is not a stop: the run completes, and says why.
    expect(terminal(bus).stopReason).toBe('cost_budget');
    expect(terminal(bus).state).toBe('COMPLETED');
    expect((await runtime.getThreadUsage(ran.threadId))!.tokens.totalTokens).toBe(30);
  });
});

describe('compaction (§2.6, §4)', () => {
  it('uses the compaction model from config and bills it under its own kind', async () => {
    const long = 'word '.repeat(4_000);
    const { runtime, storage, queue, resolved } = await makeRuntime([{ text: 'done' }], {
      pricer: priceList,
      config: {
        // Small enough that the second run's history has to be compacted.
        contextCeilingTokens: 2_000,
        contextOutputReserveTokens: 200,
        // The point of the test: a registry that has never heard of
        // 'gpt-4o-mini' names its own summariser.
        compactionModel: 'cheap-summariser',
      },
    });
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });

    const ran = await chat.run({ prompt: long });
    await runtime.worker.handleJob(queue.items[0]!);
    await chat.run({ threadId: ran.threadId, prompt: long });
    await runtime.worker.handleJob(queue.items[1]!);

    expect(resolved).toContain('cheap-summariser');
    const compactions = storage.usage.recorded.filter((u) => u.kind === 'compaction');
    expect(compactions.length).toBeGreaterThan(0);
    for (const c of compactions) {
      expect(c.model).toBe('cheap-summariser');
      expect(c.step).toBe(0);
      // The platform's own housekeeping call is priced like any other — and
      // this table has no price for it, so it is stored unpriced rather than
      // silently free.
      expect(c.cost).toBeFalsy();
    }
  });

  it('names the key when the compaction model cannot be resolved', async () => {
    // resolveModel throws from deep inside compaction, on a run that never
    // mentioned this model, so the reason that reaches the operator has to say
    // where it came from. It arrives as the FAILED run's error, not as a throw
    // — the §2.8 policy catches it.
    const { runtime, bus, queue } = await makeRuntime([{ text: 'done' }], {
      config: {
        contextCeilingTokens: 2_000,
        contextOutputReserveTokens: 200,
        compactionModel: 'unknown-summariser',
        runMaxAttempts: 1, // fail on the first attempt rather than redriving
      },
    });
    const chat = runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const long = 'word '.repeat(4_000);
    const ran = await chat.run({ prompt: long });
    await runtime.worker.handleJob(queue.items[0]!);
    await chat.run({ threadId: ran.threadId, prompt: long });
    await runtime.worker.handleJob(queue.items[1]!);

    const last = terminal(bus);
    expect(last.state).toBe('FAILED');
    expect(last.error).toContain('compactionModel');
    expect(last.error).toContain('unknown-summariser');
    expect(ran.threadId).toBeTruthy();
  });
});

describe('the pricers that ship (§4)', () => {
  const prices = pricing.table({
    'claude-sonnet-4': {
      inputPerMillion: 3, cacheReadPerMillion: 0.3,
      cacheWritePerMillion: 3.75, outputPerMillion: 15,
    },
    'gpt-4o-2024-11-20': { inputPerMillion: 2.5, outputPerMillion: 10 },
  });
  const call = (over: Partial<Parameters<Pricer['price']>[0]> = {}) => ({
    kind: 'step' as const, step: 1, outcome: 'finished' as const,
    inputTokens: 0, cacheReadInputTokens: 0, cacheWriteInputTokens: 0,
    outputTokens: 0, reasoningTokens: 0, totalTokens: 0,
    ...over,
  });

  it('prices per million tokens into micros', async () => {
    // 1M input at $3 is $3.00, which is 3_000_000 micros.
    expect(await prices.price(call({ model: 'claude-sonnet-4', inputTokens: 1_000_000 })))
      .toEqual({ micros: 3_000_000, currency: 'USD', source: 'table' });
    // A realistic mixed call: 12k fresh input, 40k cache reads, 8k cache
    // writes, 900 output.
    //   12_000×3 + 40_000×0.3 + 8_000×3.75 + 900×15 = 36_000 + 12_000 + 30_000 + 13_500
    expect(
      (await prices.price(call({
        model: 'claude-sonnet-4', inputTokens: 12_000, cacheReadInputTokens: 40_000,
        cacheWriteInputTokens: 8_000, outputTokens: 900,
      })))!.micros,
    ).toBe(91_500);
  });

  it('falls back to the wire id, then the base key, then gives up', async () => {
    expect((await prices.price(call({
      model: 'fast', modelId: 'gpt-4o-2024-11-20', outputTokens: 1_000_000,
    })))!.micros).toBe(10_000_000);
    expect((await prices.price(call({
      model: 'claude-sonnet-4@high', inputTokens: 1_000_000,
    })))!.micros).toBe(3_000_000);
    // A model nobody knows is not priced, rather than priced at zero.
    expect(await prices.price(call({ model: 'who-knows', inputTokens: 1_000 }))).toBeNull();
  });

  it('reads a receipt the provider already computed', async () => {
    const p = pricing.receipt((meta) => {
      const headers = meta.responseHeaders as Record<string, string> | undefined;
      return headers?.['x-cost-micros'] ? Number(headers['x-cost-micros']) : null;
    });
    expect(await p.price(call({
      providerMetadata: { responseHeaders: { 'x-cost-micros': '4200' } },
    }))).toEqual({ micros: 4200, currency: 'USD', source: 'receipt' });
    // No receipt on this call: say nothing, so the next pricer can try.
    expect(await p.price(call({ providerMetadata: {} }))).toBeNull();
  });

  it('chains: first answer wins, a thrower is skipped', async () => {
    const boom: Pricer = { price: () => { throw new Error('price service down'); } };
    const silent: Pricer = { price: () => null };

    expect((await pricing.chain(boom, silent, prices).price(
      call({ model: 'claude-sonnet-4', inputTokens: 1_000_000 }),
    ))!.micros).toBe(3_000_000);
    // Nobody could price it: the error comes back, so a caller can see that
    // pricing is broken rather than that the call was free.
    await expect(pricing.chain(boom, silent).price(call())).rejects.toThrow('price service down');
    // Nothing to say and nothing wrong is not an error.
    expect(await pricing.chain(silent).price(call())).toBeNull();
  });

  it('converts between micros and amounts', () => {
    expect(pricing.micros(0.25)).toBe(250_000);
    expect(pricing.amount(250_000)).toBe(0.25);
    expect(pricing.format(250_000)).toBe('0.2500 USD');
  });
});
