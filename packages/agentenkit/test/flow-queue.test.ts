import { describe, expect, it } from 'bun:test';
import { Flow } from '@clevix/flow';
import { FlowQueue, agentRunFunction, type FlowJobData } from '../src/adapters/flow.js';
import { DuplicateJobError, PayloadTooLargeError, PRIORITY_LOW } from '../src/ports/queue.js';
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

  function step(log: string[]) {
    return {
      run: async (id: string, fn: () => unknown) => {
        log.push(`run:${id}`);
        return fn();
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
      concurrency: { limit: 5, key: 'event.data.job.partitionKey' },
      timeouts: { step: '14m' },
    });
    expect(fn.failure?.id).toBe('agentenkit-run-failure');
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
