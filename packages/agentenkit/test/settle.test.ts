import { afterAll, describe, expect, it } from 'bun:test';
import { simulateReadableStream, tool } from 'ai';
import type { LanguageModelV1StreamPart } from '@ai-sdk/provider';
import { MockLanguageModelV1 } from 'ai/test';
import { z } from 'zod';
import { setupAgentCore } from '../src/runtime.js';
import { MemoryAdminStore } from '../src/admin/memory.js';
import { SqliteAdminStore } from '../src/admin/sqlite.js';
import { PostgresAdminStore, type PgLike } from '../src/admin/postgres.js';
import { MemoryBus, MemoryKv, MemoryQueue, MemoryStorage } from '../src/adapters/memory.js';
import { markRequiresConfirmation } from '../src/core/engine.js';
import { SETTLE_CLAIM_TTL_MS } from '../src/core/settle.js';
import { runLockKey } from '../src/core/lease.js';
import { resolveConfig, type AgentConfig, type NewUsage } from '../src/core/types.js';
import type { AdminStore } from '../src/ports/admin.js';
import type { Pricer, RunFinishInfo, RuntimeOptions } from '../src/ports/runtime.js';
import * as pricing from '../src/pricing.js';

// Workstream H: a run settles exactly once whatever way it ends, its caps
// count every segment, and a retry never asks the model twice. The same
// cases run in the Go package (stop_settle_test.go, spec_hooks_test.go,
// queue_hardening_test.go and settle_claim_test.go).

interface Step {
  text?: string;
  toolCalls?: Array<{ toolCallId: string; toolName: string }>;
  delayMs?: number;
  error?: string;
  noFinish?: boolean;
  usage?: [number, number];
}

function scriptedModel(steps: Step[]) {
  let call = 0;
  const model = new MockLanguageModelV1({
    provider: 'mock',
    modelId: 'mock-scripted',
    doStream: async ({ abortSignal }: any) => {
      const s = steps[Math.min(call++, steps.length - 1)]!;
      if (s.delayMs) {
        await new Promise<void>((resolve, reject) => {
          const timer = setTimeout(resolve, s.delayMs);
          abortSignal?.addEventListener('abort', () => {
            clearTimeout(timer);
            reject(Object.assign(new Error('aborted'), { name: 'AbortError' }));
          });
        });
      }
      if (s.error) throw new Error(s.error);
      const chunks: LanguageModelV1StreamPart[] = [];
      if (s.text) chunks.push({ type: 'text-delta', textDelta: s.text });
      for (const c of s.toolCalls ?? []) {
        chunks.push({ type: 'tool-call', toolCallType: 'function', toolCallId: c.toolCallId, toolName: c.toolName, args: '{}' });
      }
      if (!s.noFinish) {
        const [promptTokens, completionTokens] = s.usage ?? [10, 5];
        chunks.push({
          type: 'finish', finishReason: s.toolCalls?.length ? 'tool-calls' : 'stop',
          usage: { promptTokens, completionTokens },
        });
      }
      return { stream: simulateReadableStream({ chunks }), rawCall: { rawPrompt: null, rawSettings: {} } };
    },
  });
  return { model, calls: () => call };
}

async function makeRuntime(
  steps: Step[],
  opts: { config?: Partial<AgentConfig>; pricer?: Pricer; storage?: MemoryStorage } = {},
) {
  const { model, calls } = scriptedModel(steps);
  const storage = opts.storage ?? new MemoryStorage();
  const bus = new MemoryBus();
  const queue = new MemoryQueue();
  const kv = new MemoryKv();
  const admin = new MemoryAdminStore();
  const deps: RuntimeOptions = {
    storage, bus, queue, kv, admin,
    resolveModel: () => ({ instance: () => model, contextWindow: 128_000 }),
    config: resolveConfig({ stopPollMs: 5, runRetryBackoffMs: 0, ...opts.config }),
    ...(opts.pricer ? { pricer: opts.pricer } : {}),
  };
  const runtime = await setupAgentCore(deps);
  const handleNext = async () => {
    const job = queue.items.shift();
    queue.delays.shift();
    if (!job) throw new Error('no job queued');
    await runtime.worker.handleJob(job);
  };
  const drain = () => queue.drain((job) => runtime.worker.handleJob(job).then(() => undefined));
  const events = (threadId: string, type: string) =>
    bus.published.filter((e) => e.threadId === threadId && e.type === type);
  const lastTerminal = (threadId: string) =>
    events(threadId, 'STATE_CHANGE')
      .map((e) => e.payload as Record<string, unknown>)
      .filter((p) => ['COMPLETED', 'FAILED', 'CANCELLED'].includes(p.state as string))
      .at(-1);
  const state = async (threadId: string) => (await storage.threads.get(threadId))!.state;
  return { runtime, storage, queue, kv, admin, calls, handleNext, drain, events, lastTerminal, state };
}

const wipe = markRequiresConfirmation(tool({ parameters: z.object({}), execute: async () => 'wiped' }));
const probe = tool({ parameters: z.object({}), execute: async () => 'ok' });
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const waitFor = async (cond: () => boolean) => {
  for (let i = 0; i < 400 && !cond(); i++) await sleep(5);
  if (!cond()) throw new Error('condition never held');
};

/** Counts settles and keeps what the last one saw. */
function settleSpy() {
  const spy = { n: 0, finished: 0, last: null as RunFinishInfo | null };
  return {
    spy,
    onSettle: (info: RunFinishInfo) => { spy.n++; spy.last = info; },
    onFinish: () => { spy.finished++; },
  };
}

describe('stop settles (§5.6)', () => {
  for (const parked of [false, true]) {
    it(`a stop settles a ${parked ? 'parked' : 'queued'} run exactly once`, async () => {
      const r = await makeRuntime([{ toolCalls: [{ toolCallId: 'c1', toolName: 'wipe' }], usage: [20, 7] }], {
        config: { hitlTtlMs: 60 * 60_000 },
      });
      const { spy, onSettle, onFinish } = settleSpy();
      const chat = r.runtime.createStreamTextAgent({ name: 'chat', tools: { wipe }, onSettle, onFinish });
      const ran = await chat.run({ prompt: 'go' });
      if (parked) {
        await r.handleNext();
        expect(await r.state(ran.threadId)).toBe('WAITING_FOR_INPUT');
      }
      expect(spy.n).toBe(0); // nothing settled before the stop

      expect((await chat.stop(ran.threadId)).accepted).toBe(true);
      expect(spy.n).toBe(1); // the stop settled the run
      expect(spy.finished).toBe(1); // and finished it
      expect(spy.last).toMatchObject({ runId: ran.runId, cancelled: true, state: 'CANCELLED', stopReason: 'cancelled' });
      expect(spy.last!.usageError).toBeUndefined();
      if (parked) {
        expect(spy.last!.usage.totalTokens).toBe(27); // the parked segment's usage is on the bill
        expect(spy.last!.tokensUsed).toBe(27);
      } else {
        expect(spy.last!.usage.totalTokens).toBe(0); // a queued run spent nothing
      }
      expect((await r.admin.runs.get(ran.runId!))!.settledAt).toBeTruthy();
      expect(await r.kv.get(runLockKey(ran.threadId))).toBeNull(); // released after the settle

      // The original job (queued) or the park's expiry job (parked) is still
      // queued: a worker that wakes up later settles nothing.
      await r.drain();
      expect(spy.n).toBe(1);
      expect(spy.finished).toBe(1);
    });
  }

  it('a stop during a live step settles in the worker only', async () => {
    const r = await makeRuntime([{ text: 'slow', delayMs: 500 }]);
    const { spy, onSettle } = settleSpy();
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', onSettle });
    const ran = await chat.run({ prompt: 'hi' });
    const running = r.handleNext();
    await waitFor(() => r.calls() === 1);
    await chat.stop(ran.threadId);
    await running;
    expect(spy.n).toBe(1); // settled once, by the worker
    expect(spy.last).toMatchObject({ cancelled: true, runId: ran.runId });
    expect((await r.admin.runs.get(ran.runId!))!.settledAt).toBeTruthy();
  });

  it('a stop settles with the hook of the agent whose run it was', async () => {
    const r = await makeRuntime([{ text: 'never' }]);
    const { spy, onSettle } = settleSpy();
    const planner = r.runtime.createStreamTextAgent({ name: 'planner', onSettle });
    const other = r.runtime.createStreamTextAgent({ name: 'other' });
    const ran = await planner.run({ prompt: 'plan' });
    await other.stop(ran.threadId);
    expect(spy.n).toBe(1); // the planner's hook ran
    expect(spy.last!.runId).toBe(ran.runId);
  });
});

describe('onSettle (§5.6)', () => {
  it('a finished run is marked settled', async () => {
    const r = await makeRuntime([{ text: 'ok' }]);
    const { spy, onSettle } = settleSpy();
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', onSettle });
    const ran = await chat.run({ prompt: 'hi' });
    await r.handleNext();
    expect((await r.admin.runs.get(ran.runId!))!.settledAt).toBeTruthy();
    expect(spy.n).toBe(1);
  });

  it('runs before the terminal state is written', async () => {
    const r = await makeRuntime([{ text: 'ok', usage: [7, 3] }]);
    let seen: RunFinishInfo | null = null;
    let stateAtSettle = '';
    let terminalsAtSettle = 0;
    const chat = r.runtime.createStreamTextAgent({
      name: 'chat',
      onSettle: async (info) => {
        seen = info;
        stateAtSettle = await r.state(info.threadId);
        terminalsAtSettle = r.lastTerminal(info.threadId) ? 1 : 0;
      },
    });
    const ran = await chat.run({ prompt: 'hi' });
    await r.handleNext();
    expect(seen).toMatchObject({ runId: ran.runId, state: 'COMPLETED', tokensUsed: 10, cancelled: false });
    expect(stateAtSettle).toBe('RUNNING');
    expect(terminalsAtSettle).toBe(0);
    expect(r.lastTerminal(ran.threadId)?.state).toBe('COMPLETED');
  });

  it('an error fails the run and keeps why', async () => {
    const r = await makeRuntime([{ text: 'ok' }]);
    let finished: RunFinishInfo | null = null;
    const chat = r.runtime.createStreamTextAgent({
      name: 'chat',
      onSettle: () => { throw new Error('commit refused'); },
      onFinish: (info) => { finished = info; },
    });
    const ran = await chat.run({ prompt: 'hi' });
    await r.handleNext();
    expect(await r.state(ran.threadId)).toBe('FAILED');
    expect(r.lastTerminal(ran.threadId)).toMatchObject({ state: 'FAILED', error: 'commit refused' });
    const rec = await r.admin.runs.get(ran.runId!);
    expect(rec).toMatchObject({ state: 'FAILED', error: 'commit refused' });
    expect(finished).toMatchObject({ state: 'FAILED', error: 'commit refused' });
    expect(r.queue.items).toHaveLength(0); // a settle failure is not retried
  });

  it('sees a stop as cancelled', async () => {
    const r = await makeRuntime([{ text: 'slow', delayMs: 500 }]);
    let seen: RunFinishInfo | null = null;
    const chat = r.runtime.createStreamTextAgent({
      name: 'chat',
      onSettle: (info) => { seen = info; throw new Error('ignored on a stop'); },
    });
    const ran = await chat.run({ prompt: 'hi' });
    const running = r.handleNext();
    await waitFor(() => r.calls() === 1);
    await chat.stop(ran.threadId);
    await running;
    expect(seen).toMatchObject({ cancelled: true, state: 'CANCELLED' });
    expect(await r.state(ran.threadId)).toBe('CANCELLED'); // a settle error cannot turn a stop into a failure
  });

  it('runs when attempts are exhausted', async () => {
    const r = await makeRuntime([{ error: 'boom' }], { config: { runMaxAttempts: 1 } });
    let seen: RunFinishInfo | null = null;
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', onSettle: (info) => { seen = info; } });
    const ran = await chat.run({ prompt: 'hi' });
    await r.handleNext();
    expect(await r.state(ran.threadId)).toBe('FAILED');
    expect(seen).toMatchObject({ runId: ran.runId, state: 'FAILED', error: 'boom' });
  });
});

describe('late settle (§5.6)', () => {
  it('a run that ended under a held lock is settled once the lock clears', async () => {
    let settled = 0;
    const r = await makeRuntime([{ text: 'ok' }], { config: { runLockLeaseSeconds: 1 } });
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', onSettle: () => { settled++; } });
    const ran = await chat.run({ prompt: 'go' });
    await r.kv.set(runLockKey(ran.threadId), ran.runId!, { onlyIfNotExists: true, exSeconds: 0.25 });
    await chat.stop(ran.threadId);
    expect(settled).toBe(0); // the stop could not settle: a worker seemed to hold the run
    await r.handleNext(); // the queued job meets the held lock
    expect(r.queue.items).toHaveLength(1); // the settle is retried, not dropped
    expect(r.queue.delays[0]).toBe(1); // after the lock has surely cleared
    await sleep(300);
    await r.handleNext();
    expect(settled).toBe(1); // settled late
    expect((await r.admin.runs.get(ran.runId!))!.settledAt).toBeTruthy();
  });

  it('a failed hook leaves the run unsettled and the sweep retries it', async () => {
    let calls = 0;
    const r = await makeRuntime([{ text: 'ok' }]);
    const chat = r.runtime.createStreamTextAgent({
      name: 'chat',
      onSettle: () => { if (++calls === 1) throw new Error('ledger down'); },
    });
    const ran = await chat.run({ prompt: 'go', state: { tenant: 'acme' } });
    await r.handleNext();
    let rec = await r.admin.runs.get(ran.runId!);
    expect(rec!.state).toBe('FAILED'); // a settle failure fails the run
    expect(rec!.settledAt).toBeFalsy();
    const report = await r.runtime.reclaimStuckRuns(0);
    expect(report.settled).toBe(1);
    expect(calls).toBe(2); // the hook ran again
    rec = await r.admin.runs.get(ran.runId!);
    expect(rec!.settledAt).toBeTruthy();
  });

  it('the sweep settles a run with no recorded state', async () => {
    let calls = 0;
    const r = await makeRuntime([{ text: 'ok' }]);
    const chat = r.runtime.createStreamTextAgent({
      name: 'chat',
      onSettle: () => { if (++calls === 1) throw new Error('ledger down'); },
    });
    const ran = await chat.run({ prompt: 'go' }); // no state
    await r.handleNext();
    const rec = await r.admin.runs.get(ran.runId!);
    expect(rec!.runState ?? null).toBeNull();
    expect(rec!.settledAt).toBeFalsy();
    expect((await r.runtime.reclaimStuckRuns(0)).settled).toBe(1);
    expect(calls).toBe(2);
  });

  it('the sweep reads every page', async () => {
    let calls = 0;
    const r = await makeRuntime([{ text: 'never' }]);
    r.runtime.createStreamTextAgent({ name: 'chat', onSettle: () => { calls++; } });
    const n = 501;
    const endedAt = new Date(Date.now() - 60_000);
    for (let i = 0; i < n; i++) {
      const id = `page-${String(i).padStart(3, '0')}`;
      await r.admin.runs.start({ id, threadId: `t-${id}`, agent: 'chat', model: 'gpt-4o' });
      await r.admin.runs.patch(id, { state: 'COMPLETED', endedAt });
    }
    await sleep(5);
    const report = await r.runtime.reclaimStuckRuns(0);
    expect(report.settled).toBe(n);
    expect(calls).toBe(n);
  });

  it('two settlers at once bill once', async () => {
    let calls = 0;
    let release = () => {};
    const r = await makeRuntime([{ text: 'ok' }]);
    const chat = r.runtime.createStreamTextAgent({
      name: 'chat',
      onSettle: async () => {
        calls++;
        await new Promise<void>((resolve) => { release = resolve; });
        throw new Error('first settle fails, so the run stays unsettled');
      },
    });
    await chat.run({ prompt: 'go' });
    const worker = r.handleNext();
    await waitFor(() => calls === 1);
    release();
    await worker;

    calls = 0;
    const sweeps = [r.runtime.reclaimStuckRuns(0), r.runtime.reclaimStuckRuns(0)];
    await waitFor(() => calls === 1);
    await sleep(50);
    release();
    const [a, b] = await Promise.all(sweeps);
    expect(calls).toBe(1); // the hook ran once for both sweeps
    expect(a!.settled + b!.settled).toBe(1);
  });
});

describe('the settle claim (§5.6)', () => {
  const stores: Record<string, () => Promise<AdminStore>> = {
    memory: async () => new MemoryAdminStore(),
    sqlite: async () => {
      const { Database } = await import('bun:sqlite');
      return SqliteAdminStore.open(new Database(':memory:') as any);
    },
    // Run when TEST_ADMIN_PG is set, as the Go suite does.
    postgres: async () => {
      const { Pool } = await import('pg');
      const pool = new Pool({ connectionString: process.env.TEST_ADMIN_PG });
      pools.push(pool);
      // A clean slate, as test/postgres-admin.test.ts does: the Go suite may
      // have migrated this database, and the two 0001 migrations still differ.
      await pool.query('DROP TABLE IF EXISTS agentic_steps, agentic_runs, agentic_threads, agentic_migrations');
      return PostgresAdminStore.connect(pool as PgLike);
    },
  };
  const pools: Array<{ end(): Promise<void> }> = [];
  afterAll(async () => {
    await Promise.all(pools.map((p) => p.end().catch(() => undefined)));
  });
  for (const [name, open] of Object.entries(stores)) {
    const run = name === 'postgres' && !process.env.TEST_ADMIN_PG ? it.skip : it;
    run(`only one claim wins and a stale one is taken over (${name})`, async () => {
      const runs = (await open()).runs;
      const id = `claim-${crypto.randomUUID()}`;
      await runs.start({ id, threadId: `t-${id}`, agent: 'chat', model: 'm' });
      const fresh = new Date(Date.now() - SETTLE_CLAIM_TTL_MS);
      expect(await runs.claimSettle(id, 'a', fresh)).toBe(true); // the first claim wins
      expect(await runs.claimSettle(id, 'b', fresh)).toBe(false); // a second loses while the first is fresh
      // The first settler died: a claim older than the cut-off is taken over.
      expect(await runs.claimSettle(id, 'c', new Date(Date.now() + 1_000))).toBe(true);
      // The dead settler's late mark is not believed: the claim is not its own.
      await runs.endSettle(id, 'a', true);
      expect((await runs.get(id))!.settledAt).toBeFalsy();
      await runs.endSettle(id, 'c', true);
      const rec = await runs.get(id);
      expect(rec!.settledAt).toBeTruthy();
      expect(rec!.settlingAt ?? null).toBeNull();
      expect(await runs.claimSettle(id, 'd', new Date(Date.now() + 3_600_000))).toBe(false); // never again
    });
  }
});

describe('budgets and retries (§2.1, §2.8)', () => {
  it('a run that parks three times counts every segment', async () => {
    const r = await makeRuntime([
      { toolCalls: [{ toolCallId: 'c1', toolName: 'wipe' }] },
      { toolCalls: [{ toolCallId: 'c2', toolName: 'wipe' }] },
      { toolCalls: [{ toolCallId: 'c3', toolName: 'wipe' }] },
      { text: 'done' },
    ], { config: { hitlTtlMs: 60 * 60_000 } });
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', tools: { wipe } });
    // Each step spends 15 (10 in, 5 out). The budget fits two steps, not four.
    const ran = await chat.run({ prompt: 'go', tokenBudget: 35 });
    await r.handleNext();
    for (const id of ['c1', 'c2', 'c3']) {
      expect(await r.state(ran.threadId)).toBe('WAITING_FOR_INPUT');
      await r.runtime.hitl.respond({ threadId: ran.threadId, toolCallId: id, approved: true });
      await r.handleNext();
    }
    expect(r.lastTerminal(ran.threadId)).toMatchObject({ stopReason: 'token_budget', tokensUsed: 60 });
  });

  it('a run whose last step was saved is not asked again', async () => {
    // The worker dies right after the step's messages and usage are saved.
    const storage = new MemoryStorage();
    const append = storage.events.append.bind(storage.events);
    let failed = false;
    storage.events.append = async (t, e) => {
      if (e.type === 'STEP_COMMITTED' && !failed) {
        failed = true;
        throw new Error('worker died');
      }
      return append(t, e);
    };
    const r = await makeRuntime([{ text: 'the answer' }, { text: 'a second answer' }], { storage });
    const { spy, onSettle } = settleSpy();
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', onSettle });
    const ran = await chat.run({ prompt: 'hi' });
    await r.handleNext(); // saves the answer, then dies
    expect(await r.state(ran.threadId)).toBe('QUEUED'); // waiting to retry
    await r.handleNext(); // the retry
    expect(r.calls()).toBe(1); // the model was asked once
    expect(await r.state(ran.threadId)).toBe('COMPLETED');
    const roles = (await storage.messages.list(ran.threadId, { agentId: null })).map((m) => m.role);
    expect(roles).toEqual(['user', 'assistant']); // one answer
    expect(storage.usage.recorded).toHaveLength(1); // the call billed once
    expect(spy.n).toBe(1);
  });
});

describe('money (§4)', () => {
  it('a call priced in a second currency is stored unpriced', async () => {
    let n = 0;
    const pricer: Pricer = {
      price: () => (++n === 1
        ? { micros: 100, currency: 'USD', source: 'table' }
        : { micros: 900, currency: 'EUR', source: 'table' }),
    };
    const r = await makeRuntime([{ toolCalls: [{ toolCallId: 'c1', toolName: 'probe' }] }, { text: 'done' }], { pricer });
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', tools: { probe } });
    const ran = await chat.run({ prompt: 'go' });
    await r.handleNext();
    const rows = r.storage.usage.recorded as NewUsage[];
    expect(rows).toHaveLength(2);
    expect(rows[1]!.cost ?? null).toBeNull(); // the euro call is stored unpriced
    const bill = await r.storage.usage.total(ran.threadId, { runId: ran.runId });
    expect(bill.costs).toHaveLength(1); // one currency on the run's bill
    expect(bill.unpriced).toBe(1); // the refused call is a gap in it
  });

  it('a thread detail carries its cost', async () => {
    const r = await makeRuntime([{ text: 'done' }], {
      pricer: pricing.table({ 'gpt-4o': { inputPerMillion: 10, outputPerMillion: 30 } }),
    });
    const chat = r.runtime.createStreamTextAgent({ name: 'chat', model: 'gpt-4o' });
    const ran = await chat.run({ prompt: 'go' });
    await r.handleNext();
    const detail = await r.runtime.admin.getThread(ran.threadId);
    expect(detail!.thread.tokens.costMicros).toBe(250);
    expect(detail!.thread.tokens.currency).toBe('USD');
  });
});
