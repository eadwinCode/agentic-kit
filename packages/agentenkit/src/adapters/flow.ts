import type { ActiveRunStatus, Duration, Flow, FlowEnv, FlowFunction, RunSummary } from '@clevix/flow';
import { DuplicateJobError, PayloadTooLargeError, type EnqueueOptions, type Queue, type QueueStats, type QueuedJob } from '../ports/queue.js';
import type { AgentCore } from '../ports/runtime.js';
import type { JobKind, RunJob } from '../core/types.js';

/** The function ID the queue starts and the worker serves, unless both are
 *  given another. */
export const FLOW_FUNCTION_ID = 'agentenkit-run';

/** The event the function lists as its trigger. Nothing sends it: runs come
 *  from `flow.start`. Flow needs every function to have a trigger. */
export const FLOW_JOB_EVENT = 'agentenkit/job';

/** A run's data is at most 1 MB on Flow. */
const MAX_DATA_BYTES = 1_000_000;

/** Flow's `priority.run` is in seconds, from -600 to 600. */
const PRIORITY_SECONDS = 60;
const MAX_PRIORITY = 600;

const ACTIVE: ActiveRunStatus[] = ['queued', 'running', 'sleeping', 'waiting', 'waiting_for_app', 'paused'];

/** What one run of the function carries. */
export interface FlowJobData {
  job: RunJob;
  /** The singleton key: the enqueue's key, or the job's dispatch id. */
  key: string;
  /** In seconds, for `priority.run`. */
  priority: number;
  /** Epoch ms. The run sleeps until then before it runs the job. */
  notBefore?: number;
}

export interface FlowQueueOptions {
  /** The function to start. Defaults to `agentenkit-run`. Must match the
   *  ID given to `agentRunFunction`. */
  functionId?: string;
  /** Cloudflare Workers: the handler's `env`, where the Flow keys live. */
  env?: FlowEnv;
}

/** Queue adapter over Clevix Flow.
 *
 *  Each job is one run of one Flow function (see `agentRunFunction`),
 *  started with `flow.start`. A key is the function's singleton key in
 *  `skip` mode, so at most one live run holds it, and it is free again as
 *  soon as that run ends. A delay is a sleep at the start of the run, so the
 *  key stays held while the job waits. `cancel` cancels the run that holds a
 *  key. `find` and `stats` read Flow's runs index, which lags the runs by a
 *  few seconds.
 *
 *  Needs Flow SDK 0.7.0 or later and an engine with the run routes. */
export class FlowQueue implements Queue {
  private readonly functionId: string;

  constructor(private readonly flow: Flow, private readonly opts: FlowQueueOptions = {}) {
    this.functionId = opts.functionId ?? FLOW_FUNCTION_ID;
  }

  private get env(): { env?: FlowEnv } {
    return this.opts.env ? { env: this.opts.env } : {};
  }

  async enqueue(job: RunJob, opts?: EnqueueOptions): Promise<void> {
    // An unkeyed job still needs a singleton key, or every unkeyed job would
    // share one. Its dispatch id is its own; a second start of the same
    // dispatch is the same job, so it is dropped without an error.
    const key = opts?.key || job.dispatchId || crypto.randomUUID();
    const data: FlowJobData = {
      job,
      key,
      priority: Math.max(-MAX_PRIORITY, Math.min(MAX_PRIORITY, (opts?.priority ?? 0) * PRIORITY_SECONDS)),
      ...(opts?.delaySeconds && opts.delaySeconds > 0
        ? { notBefore: Date.now() + Math.ceil(opts.delaySeconds * 1000) }
        : {}),
    };
    if (new TextEncoder().encode(JSON.stringify(data)).length > MAX_DATA_BYTES) throw new PayloadTooLargeError();

    // Twice at most: a singleton answer with no active run means the run that
    // held the key ended in between, so the key is free now.
    for (let i = 0; i < 2; i++) {
      const res = await this.flow.start(this.functionId, { data: data as never, ...this.env });
      if (res.started) return;
      if (res.reason === 'singleton') {
        if (res.activeRunId === null) continue;
        if (opts?.key) throw new DuplicateJobError(opts.key);
        return;
      }
      if (res.reason === 'paused') throw new Error(`flow: function ${this.functionId} is paused`);
      throw new Error(`flow: no function ${this.functionId}; is the app served and synced?`);
    }
    if (opts?.key) throw new DuplicateJobError(opts.key);
  }

  async cancel(key: string): Promise<void> {
    await this.flow.runs.cancel({ function: this.functionId, singletonKey: key }, this.env);
  }

  async find(runId: string): Promise<QueuedJob | null> {
    const page = await this.flow.runs.list({
      function: this.functionId, tags: { runId }, status: ACTIVE, limit: 50, ...this.env,
    });
    // Newest first: the earliest is the last one.
    const run = page.items[page.items.length - 1];
    return run ? queuedJob(run) : null;
  }

  async stats(): Promise<QueueStats> {
    const [counts, queued] = await Promise.all([
      this.flow.runs.counts({ function: this.functionId, ...this.env }),
      this.flow.runs.list({ function: this.functionId, status: ['queued'], limit: 200, ...this.env }),
    ]);
    // Newest first. With more than one page this is the oldest of the
    // newest 200, so it can read low.
    const oldest = queued.items[queued.items.length - 1];
    return {
      ready: counts.queued + counts.waiting_for_app,
      delayed: counts.sleeping + counts.waiting,
      inFlight: counts.running,
      dead: counts.failed,
      oldestReadyMs: oldest ? Math.max(0, Date.now() - oldest.createdAt) : 0,
      paused: counts.paused > 0,
      claimErrors: 0,
    };
  }
}

function queuedJob(run: RunSummary): QueuedJob {
  return {
    id: run.id,
    runId: run.tags.runId ?? '',
    threadId: run.tags.threadId ?? '',
    kind: (run.tags.kind || undefined) as JobKind | undefined,
    attempts: 0,
    runAt: new Date(run.createdAt),
    position: -1,
  };
}

export interface AgentRunFunctionOptions {
  /** Defaults to `agentenkit-run`. Must match `FlowQueue`'s `functionId`.
   *  At most 56 characters, since the failure function adds `-failure`. */
  id?: string;
  /** How many jobs of one `partitionKey` run at once. Unset: no limit.
   *  Job priorities only take effect under a limit. */
  concurrency?: number;
  /** Flow's retries for a job that throws. Flow's default when unset. */
  retries?: number;
  /** The longest one job may run. Defaults to 14 minutes, Flow's most. A
   *  segment that runs longer is cut and retried. */
  stepTimeout?: Duration;
}

/** The Flow function that runs `FlowQueue`'s jobs. Serve it with the app's
 *  other Flow functions.
 *
 *  A job runs in one step. `handleJob` only throws for what a queue should
 *  retry (an unknown agent, a store that is down), so Flow's retries cover
 *  delivery; agentic-kit retries a failed run itself. When Flow gives up, the
 *  failure function hands the job to `handleDeadJob`, which fails the run. */
export function agentRunFunction(
  flow: Flow,
  core: Pick<AgentCore, 'worker'>,
  opts: AgentRunFunctionOptions = {},
): FlowFunction {
  return flow.createFunction(
    {
      id: opts.id ?? FLOW_FUNCTION_ID,
      singleton: { key: 'event.data.key', mode: 'skip' },
      tags: { runId: 'event.data.job.runId', threadId: 'event.data.job.threadId', kind: 'event.data.job.kind' },
      priority: { run: 'event.data.priority' },
      ...(opts.concurrency ? { concurrency: { limit: opts.concurrency, key: 'event.data.job.partitionKey' } } : {}),
      ...(opts.retries !== undefined ? { retries: opts.retries } : {}),
      timeouts: { step: opts.stepTimeout ?? '14m' },
      onFailure: async ({ event, error, attempts }) => {
        const data = (event.data?.event as { data?: FlowJobData } | undefined)?.data;
        if (!data?.job) return;
        await core.worker.handleDeadJob(data.job, attempts, new Error(error?.message ?? 'the Flow run failed'));
      },
    },
    { event: FLOW_JOB_EVENT },
    async ({ event, step }) => {
      const data = event.data as unknown as FlowJobData;
      if (data.notBefore) await step.sleepUntil('delay', data.notBefore);
      return step.run('handle', () => core.worker.handleJob(data.job));
    },
  );
}
