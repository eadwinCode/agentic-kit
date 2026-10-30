import { describe, expect, it } from 'bun:test';
import { Flow } from '@clevix/flow';
import { FlowQueue, agentRunFunction, retryBackoff, type FlowJobData } from '../src/adapters/flow.js';
import { DuplicateJobError, PayloadTooLargeError, PRIORITY_LOW, QueueFullError } from '../src/ports/queue.js';
import type { RunJob } from '../src/core/types.js';

interface Call {
  method: string;
  path: string;
  query: URLSearchParams;
  body: any;
}

/** A Flow client whose engine is a list of canned answers, one per call. */
function fakeFlow(answers: unknown[]) {
  const calls: Call[] = [];
  const flow = new Flow({
    id: 'app',
    baseUrl: 'https://engine.test',
    fetch: (async (input: string | URL | Request, init?: RequestInit) => {
      const url = new URL(String(input));
      calls.push({
        method: init?.method ?? 'GET',
        path: url.pathname,
        query: url.searchParams,
        body: init?.body ? JSON.parse(String(init.body)) : undefined,
      });
      const answer = answers.shift();
      if (answer === undefined) throw new Error(`no answer left for ${url.pathname}`);
      return new Response(JSON.stringify(answer), { status: 200, headers: { 'Content-Type': 'application/json' } });
    }) as typeof fetch,
  });
  return { flow, calls };
}

const job: RunJob = { threadId: 't1', runId: 'r1', dispatchId: 'd1', model: 'm' };

describe('FlowQueue.enqueue', () => {
  it('starts one run with the job, its key and its priority', async () => {
    const { flow, calls } = fakeFlow([{ started: true, runId: 'run_1', eventId: 'evt_1' }]);
    await new FlowQueue(flow).enqueue(job, { key: 'hitl-resume:c1', priority: PRIORITY_LOW });
    expect(calls).toHaveLength(1);
    expect(calls[0]!.method).toBe('POST');
    expect(calls[0]!.path).toBe('/v1/functions/agentenkit-run/start');
    expect(calls[0]!.body.data).toEqual({ job, key: 'hitl-resume:c1', priority: -600 });
    // No id: a key must be free again once its run ends.
    expect(calls[0]!.body.id).toBeUndefined();
  });

  it('uses the dispatch id as the key of an unkeyed job', async () => {
    const { flow, calls } = fakeFlow([{ started: true, runId: 'run_1', eventId: 'evt_1' }]);
    await new FlowQueue(flow, { functionId: 'mine' }).enqueue(job);
    expect(calls[0]!.path).toBe('/v1/functions/mine/start');
    expect(calls[0]!.body.data.key).toBe('d1');
    expect(calls[0]!.body.data.priority).toBe(0);
  });

  it('turns a delay into a not-before time', async () => {
    const { flow, calls } = fakeFlow([{ started: true, runId: 'run_1', eventId: 'evt_1' }]);
    const before = Date.now();
    await new FlowQueue(flow).enqueue(job, { delaySeconds: 30 });
    const notBefore = calls[0]!.body.data.notBefore as number;
    expect(notBefore).toBeGreaterThanOrEqual(before + 30_000);
    expect(notBefore).toBeLessThanOrEqual(Date.now() + 30_000);
  });

  it('throws DuplicateJobError when a live run holds the key', async () => {
    const { flow } = fakeFlow([{ started: false, reason: 'singleton', activeRunId: 'run_0' }]);
    await expect(new FlowQueue(flow).enqueue(job, { key: 'k' })).rejects.toBeInstanceOf(DuplicateJobError);
  });

  it('drops a second start of the same unkeyed dispatch without an error', async () => {
    const { flow } = fakeFlow([{ started: false, reason: 'singleton', activeRunId: 'run_0' }]);
    await new FlowQueue(flow).enqueue(job);
  });

  it('starts again when the run that held the key ended in between', async () => {
    const { flow, calls } = fakeFlow([
      { started: false, reason: 'singleton', activeRunId: null },
      { started: true, runId: 'run_2', eventId: 'evt_2' },
    ]);
    await new FlowQueue(flow).enqueue(job, { key: 'k' });
    expect(calls).toHaveLength(2);
  });

  it('throws when no run starts after two tries', async () => {
    const freeing = { started: false, reason: 'singleton', activeRunId: null };
    const keyed = fakeFlow([freeing, freeing]);
    await expect(new FlowQueue(keyed.flow).enqueue(job, { key: 'k' })).rejects.toThrow('no run started for key k');
    expect(keyed.calls).toHaveLength(2);
    const unkeyed = fakeFlow([freeing, freeing]);
    await expect(new FlowQueue(unkeyed.flow).enqueue(job)).rejects.toThrow('no run started for key d1');
  });

  it('throws when the function is paused or missing', async () => {
    const paused = fakeFlow([{ started: false, reason: 'paused' }]);
    await expect(new FlowQueue(paused.flow).enqueue(job)).rejects.toThrow('paused');
    const missing = fakeFlow([{ started: false, reason: 'not_found' }]);
    await expect(new FlowQueue(missing.flow).enqueue(job)).rejects.toThrow('no function');
  });

  it('refuses a job over 1 MB before calling the engine', async () => {
    const { flow, calls } = fakeFlow([]);
    const big = { ...job, state: { blob: 'x'.repeat(1_000_001) } };
    await expect(new FlowQueue(flow).enqueue(big)).rejects.toBeInstanceOf(PayloadTooLargeError);
    expect(calls).toHaveLength(0);
  });
});

describe('FlowQueue reads', () => {
  it('cancel cancels the run that holds the key', async () => {
    const { flow, calls } = fakeFlow([{ cancelled: ['run_1'], notActive: [], more: false, next: null }]);
    await new FlowQueue(flow).cancel('hitl-expiry:c1');
    expect(calls[0]!.path).toBe('/v1/runs/cancel');
    expect(calls[0]!.body).toMatchObject({ function: 'agentenkit-run', singletonKey: 'hitl-expiry:c1' });
  });

  it('find looks a run up by its runId tag and gives the earliest', async () => {
    const { flow, calls } = fakeFlow([{
      items: [
        { id: 'run_new', functionId: 'agentenkit-run', status: 'sleeping', createdAt: 2000, endedAt: null, tags: { runId: 'r1', threadId: 't1', kind: 'expiry' } },
        { id: 'run_old', functionId: 'agentenkit-run', status: 'running', createdAt: 1000, endedAt: null, tags: { runId: 'r1', threadId: 't1' } },
      ],
      next: null,
    }]);
    const found = await new FlowQueue(flow).find('r1');
    expect(calls[0]!.path).toBe('/v1/runs');
    expect(calls[0]!.query.get('function')).toBe('agentenkit-run');
    expect(calls[0]!.query.get('tag.runId')).toBe('r1');
    expect(calls[0]!.query.get('status')).toBe('queued,running,sleeping,waiting,waiting_for_app,paused');
    expect(found).toEqual({
      id: 'run_old', runId: 'r1', threadId: 't1', kind: undefined, attempts: 0, runAt: new Date(1000), position: -1,
    });
  });

  it('find gives null when no active run has the tag', async () => {
    const { flow } = fakeFlow([{ items: [], next: null }]);
    expect(await new FlowQueue(flow).find('r1')).toBeNull();
  });

  it('stats maps the run counts', async () => {
    const now = Date.now();
    const { flow } = fakeFlow([
      { queued: 3, running: 2, sleeping: 4, waiting: 1, waiting_for_app: 1, paused: 0, completed: 9, failed: 5, cancelled: 1, since: 0 },
      { items: [{ id: 'run_q', functionId: 'agentenkit-run', status: 'queued', createdAt: now - 5000, endedAt: null, tags: {} }], next: null },
    ]);
    const stats = await new FlowQueue(flow).stats();
    expect(stats).toMatchObject({ ready: 4, delayed: 5, inFlight: 2, dead: 5, paused: false, claimErrors: 0 });
    expect(stats.oldestReadyMs).toBeGreaterThanOrEqual(5000);
  });

  it('stats asks for the oldest queued run, not the newest page', async () => {
    const { flow, calls } = fakeFlow([{ queued: 0, since: 0 }, { items: [], next: null }]);
    await new FlowQueue(flow).stats();
    expect(calls[1]!.query.get('order')).toBe('oldest');
    expect(calls[1]!.query.get('limit')).toBe('1');
  });
});

const run = (id: string, status: string, createdAt: number, endedAt: number | null, tags: Record<string, string> = {}) => ({
  id, functionId: 'agentenkit-run', status, createdAt, endedAt, tags,
});

describe('FlowQueue options', () => {
  const started = { started: true, runId: 'run_1', eventId: 'evt_1' };

  it('holds a job while the function is paused', async () => {
    const { flow, calls } = fakeFlow([started]);
    await new FlowQueue(flow).enqueue(job);
    expect(calls[0]!.body.whenPaused).toBe('hold');
  });

  it('puts the namespace in front of the function ID', async () => {
    const { flow, calls } = fakeFlow([started]);
    await new FlowQueue(flow, { namespace: 'staging' }).enqueue(job);
    expect(calls[0]!.path).toBe('/v1/functions/staging-agentenkit-run/start');
    const fn = agentRunFunction(flow, { worker: {} as never }, { namespace: 'staging' });
    expect(fn.id).toBe('staging-agentenkit-run');
    expect(fn.failure?.id).toBe('staging-agentenkit-run-failure');
  });

  it('refuses a fresh dispatch at maxDepth, and lets a retry in', async () => {
    const counts = (queued: number) => ({ queued, waiting_for_app: 1, paused: 1, since: 0 });
    let f = fakeFlow([counts(1)]);
    await expect(new FlowQueue(f.flow, { maxDepth: 3 }).enqueue(job)).rejects.toBeInstanceOf(QueueFullError);
    expect(f.calls).toHaveLength(1);

    f = fakeFlow([counts(0), started]);
    await new FlowQueue(f.flow, { maxDepth: 3 }).enqueue(job);
    expect(f.calls).toHaveLength(2);

    f = fakeFlow([started]);
    await new FlowQueue(f.flow, { maxDepth: 3 }).enqueue({ ...job, kind: 'retry' });
    expect(f.calls).toHaveLength(1);
  });
});

describe('FlowQueue admin', () => {
  it('pause and resume pause the function', async () => {
    const { flow, calls } = fakeFlow([{ paused: true, runs: 2, more: false }, { paused: false, runs: 2, more: false }]);
    const q = new FlowQueue(flow);
    await q.pause();
    await q.resume();
    expect(calls.map((c) => c.path)).toEqual(['/v1/functions/agentenkit-run/pause', '/v1/functions/agentenkit-run/resume']);
  });

  it('purge refuses an empty filter', async () => {
    const { flow } = fakeFlow([]);
    await expect(new FlowQueue(flow).purge({ includeDead: true })).rejects.toThrow('needs a filter');
  });

  it('purge cancels matching waiting runs and deletes matching failed ones', async () => {
    const now = Date.now();
    const { flow, calls } = fakeFlow([
      { items: [run('run_a', 'queued', now - 60_000, null, { threadId: 't1', kind: 'retry' }), run('run_new', 'queued', now, null, { threadId: 't1', kind: 'retry' })], next: 'cur' },
      { cancelled: ['run_a'], notActive: [] },
      { items: [run('run_b', 'sleeping', now - 60_000, null, { threadId: 't1', kind: '' })], next: null },
      { items: [run('run_f', 'failed', now - 60_000, now - 50_000, { threadId: 't1', kind: 'retry' })], next: null },
      { deleted: true },
    ]);
    const n = await new FlowQueue(flow).purge({ threadId: 't1', kind: 'retry', olderThanMs: 30_000, includeDead: true });
    expect(n).toBe(2);
    expect(calls[0]!.query.get('tag.threadId')).toBe('t1');
    expect(calls[0]!.query.get('status')).toBe('queued,sleeping,waiting,waiting_for_app,paused');
    expect(calls[1]!.path).toBe('/v1/runs/cancel');
    expect(calls[1]!.body.runId).toBe('run_a');
    expect(calls[2]!.query.get('before')).toBe('cur');
    expect(calls[4]!.path).toBe('/v1/runs/run_f/delete');
  });

  it('listDead gives the failed runs with their job, attempts and error', async () => {
    const now = Date.now();
    const data: FlowJobData = { job, key: 'd1', priority: 0 };
    const { flow, calls } = fakeFlow([
      { items: [run('run_f', 'failed', now - 9000, now - 1000), run('run_g', 'failed', now - 9000, now - 2000)], next: null },
      { ...run('run_f', 'failed', now - 9000, now - 1000), attempts: 4, error: { name: 'Error', message: 'store down' }, event: { id: 'e', name: 'agentenkit/job', data, ts: 1 } },
    ]);
    const dead = await new FlowQueue(flow).listDead(1);
    expect(dead).toEqual([{ id: 'run_f', job, attempts: 4, deadAt: new Date(now - 1000), lastError: 'store down' }]);
    expect(calls[0]!.query.get('status')).toBe('failed');
    expect(calls).toHaveLength(2);
  });

  it('redrive retries a failed run, and leaves a cancelled one', async () => {
    const detail = (status: string) => ({ ...run('run_f', status, 1, 2), attempts: 1, error: null, event: { id: 'e', name: 'n', data: {}, ts: 1 } });
    const { flow, calls } = fakeFlow([
      detail('failed'), { retried: true },
      detail('failed'), { retried: false, reason: 'paused' },
      detail('cancelled'),
    ]);
    const q = new FlowQueue(flow);
    expect(await q.redrive('run_f')).toBe(true);
    expect(await q.redrive('run_f')).toBe(false);
    // A job purge cancelled is not dead: it is not brought back.
    expect(await q.redrive('run_f')).toBe(false);
    expect(calls.map((c) => c.path)).toEqual([
      '/v1/runs/run_f', '/v1/runs/run_f/retry', '/v1/runs/run_f', '/v1/runs/run_f/retry', '/v1/runs/run_f',
    ]);
  });

  it('purge with kind null takes only fresh dispatches', async () => {
    const now = Date.now();
    const { flow, calls } = fakeFlow([
      { items: [run('run_d', 'queued', now, null, { threadId: 't1', kind: '' }), run('run_r', 'queued', now, null, { threadId: 't1', kind: 'retry' })], next: null },
      { cancelled: ['run_d'], notActive: [] },
    ]);
    expect(await new FlowQueue(flow).purge({ threadId: 't1', kind: null })).toBe(1);
    expect(calls[1]!.body.runId).toBe('run_d');
  });

  it('purgeDead deletes failed runs past the age, and counts only what went', async () => {
    const now = Date.now();
    const { flow, calls } = fakeFlow([
      { items: [run('run_old', 'failed', now - 7_200_000, now - 7_200_000), run('run_new', 'failed', now - 1000, now - 1000), run('run_kept', 'failed', now - 7_200_000, now - 7_200_000)], next: null },
      { deleted: true },
      { deleted: false, reason: 'kept' },
    ]);
    expect(await new FlowQueue(flow).purgeDead(3_600_000)).toBe(1);
    expect(calls.map((c) => c.path)).toEqual(['/v1/runs', '/v1/runs/run_old/delete', '/v1/runs/run_kept/delete']);
  });
});

describe('agentRunFunction', () => {
  function core() {
    const handled: RunJob[] = [];
    const dead: Array<{ job: RunJob; attempts: number; cause: unknown }> = [];
    return {
      handled,
      dead,
      core: {
        worker: {
          handleJob: async (j: RunJob) => {
            handled.push(j);
            return { accepted: true };
          },
          handleDeadJob: async (j: RunJob, attempts: number, cause: unknown) => {
            dead.push({ job: j, attempts, cause });
          },
        },
      },
    };
  }

  function step(log: string[], attempt = 0) {
    return {
      run: async (id: string, fn: (info: { attempt: number }) => unknown) => {
        log.push(`run:${id}`);
        return fn({ attempt });
      },
      sleepUntil: async (id: string, time: unknown) => {
        log.push(`sleepUntil:${id}:${time}`);
      },
    };
  }

  it('sets the singleton, tags, priority, concurrency and step timeout', () => {
    const { flow } = fakeFlow([]);
    const fn = agentRunFunction(flow, core().core, { concurrency: 5 });
    expect(fn.id).toBe('agentenkit-run');
    expect(fn.triggers).toEqual([{ event: 'agentenkit/job' }]);
    expect(fn.options).toMatchObject({
      singleton: { key: 'event.data.key', mode: 'skip' },
      tags: { runId: 'event.data.job.runId', threadId: 'event.data.job.threadId', kind: 'event.data.job.kind' },
      priority: { run: 'event.data.priority' },
      concurrency: [{ limit: 5, key: 'event.data.job.partitionKey' }],
      timeouts: { step: '14m' },
    });
    expect(fn.failure?.id).toBe('agentenkit-run-failure');
  });

  it('refuses a segment that is not shorter than the step', () => {
    const { flow } = fakeFlow([]);
    expect(() => agentRunFunction(flow, core().core, { segmentTimeoutMs: 60 * 60_000 })).toThrow('must be shorter');
    expect(() => agentRunFunction(flow, core().core, { segmentTimeoutMs: 10 * 60_000, stepTimeout: '1h30m' })).not.toThrow();
    expect(() => agentRunFunction(flow, core().core, { segmentTimeoutMs: 10 * 60_000 })).not.toThrow();
  });

  it('sets a global concurrency limit beside the partition one', () => {
    const { flow } = fakeFlow([]);
    const fn = agentRunFunction(flow, core().core, { concurrency: 2, globalConcurrency: 10 });
    expect(fn.options.concurrency).toEqual([{ limit: 2, key: 'event.data.job.partitionKey' }, { limit: 10 }]);
  });

  it('waits the retry backoff before the next try', async () => {
    expect([0, 1, 2, 3, 4].map((a) => retryBackoff(1000, 5000, a))).toEqual([1000, 2000, 4000, 5000, 5000]);
    const { flow } = fakeFlow([]);
    const failing = { worker: { handleJob: async () => { throw new Error('store down'); }, handleDeadJob: async () => {} } };
    const data: FlowJobData = { job, key: 'd1', priority: 0 };
    const before = Date.now();
    const err: any = await Promise.resolve(agentRunFunction(flow, failing, { retryBackoffMs: 2000 })
      .handler({ event: { data }, step: step([], 2) } as never)).catch((e: unknown) => e);
    expect(err.name).toBe('RetryAfterError');
    expect(err.message).toBe('store down');
    expect(err.retryAt).toBeGreaterThanOrEqual(before + 8000);
    expect(err.retryAt).toBeLessThanOrEqual(Date.now() + 8000);

    const { NonRetriableError } = await import('@clevix/flow');
    const final = { worker: { handleJob: async () => { throw new NonRetriableError('bad job'); }, handleDeadJob: async () => {} } };
    const kept: any = await Promise.resolve(agentRunFunction(flow, final, { retryBackoffMs: 2000 })
      .handler({ event: { data }, step: step([], 2) } as never)).catch((e: unknown) => e);
    expect(kept.name).toBe('NonRetriableError');

    const plain: any = await Promise.resolve(agentRunFunction(flow, failing)
      .handler({ event: { data }, step: step([], 2) } as never)).catch((e: unknown) => e);
    expect(plain.name).toBe('Error');
  });

  it('runs the job in one step', async () => {
    const { flow } = fakeFlow([]);
    const c = core();
    const log: string[] = [];
    const data: FlowJobData = { job, key: 'd1', priority: 0 };
    const out = await agentRunFunction(flow, c.core).handler({ event: { data }, step: step(log) } as never);
    expect(log).toEqual(['run:handle']);
    expect(c.handled).toEqual([job]);
    expect(out).toEqual({ accepted: true });
  });

  it('sleeps until not-before first', async () => {
    const { flow } = fakeFlow([]);
    const c = core();
    const log: string[] = [];
    const data: FlowJobData = { job, key: 'd1', priority: 0, notBefore: 1234 };
    await agentRunFunction(flow, c.core).handler({ event: { data }, step: step(log) } as never);
    expect(log).toEqual(['sleepUntil:delay:1234', 'run:handle']);
  });

  it('hands a failed run to handleDeadJob with its attempts', async () => {
    const { flow } = fakeFlow([]);
    const c = core();
    const data: FlowJobData = { job, key: 'd1', priority: 0 };
    const failure = agentRunFunction(flow, c.core).failure!;
    expect(failure.triggers).toEqual([
      { event: 'inngest/function.failed', if: "event.data.function_id == 'app-agentenkit-run'" },
    ]);
    await failure.handler({
      event: { data: { error: { name: 'Error', message: 'store down' }, attempts: 5, event: { data } } },
    } as never);
    expect(c.dead).toHaveLength(1);
    expect(c.dead[0]!.job).toEqual(job);
    expect(c.dead[0]!.attempts).toBe(5);
    expect(String(c.dead[0]!.cause)).toContain('store down');
  });
});
