import { RetryAfterError } from '@clevix/flow';
import type { ActiveRunStatus, Duration, Flow, FlowEnv, FlowFunction, Json, RunStatus, RunSummary, StreamItem as FlowStreamItem } from '@clevix/flow';
import {
  DuplicateJobError, PayloadTooLargeError, QueueFullError, type DeadJob, type EnqueueOptions, type PurgeFilter, type Queue,
  type QueueAdmin, type QueueStats, type QueuedJob,
} from '../ports/queue.js';
import type { AgentCore } from '../ports/runtime.js';
import type { EventBus } from '../ports/bus.js';
import type { Kv } from '../ports/kv.js';
import {
  StreamClosedError, StreamGoneError, type RunStreams, type StreamEnd, type StreamEvent, type StreamItem,
  type StreamMeta, type StreamSnapshot,
} from '../ports/streams.js';
import { isStreamEnd } from '../core/stream-events.js';
import type { AgentEvent, JobKind, RunJob } from '../core/types.js';

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

/** A run that has not started its job, or waits between tries. `purge`
 *  cancels only these. */
const WAITING: ActiveRunStatus[] = ['queued', 'sleeping', 'waiting', 'paused'];

/** The most runs one list call gives. */
const PAGE_SIZE = 200;

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
  /** Lets two deployments share one Flow app: it goes in front of the
   *  function ID, as `<namespace>-<id>`. Singleton keys belong to one
   *  function, so the keys are apart too. Must match `agentRunFunction`'s
   *  `namespace`. */
  namespace?: string;
  /** Refuses a fresh dispatch (`QueueFullError`) once this many runs wait to
   *  start. Retries, redrives, resumes and expiries always go in. The count
   *  comes from Flow's runs index, which lags a few seconds, so a burst can
   *  go a little past it. Unset: no limit. */
  maxDepth?: number;
  /** Cloudflare Workers: the handler's `env`, where the Flow keys live. */
  env?: FlowEnv;
}

/** The function's ID: `id`, or `agentenkit-run`, after the namespace. */
function functionName(namespace: string | undefined, id: string | undefined): string {
  const name = id || FLOW_FUNCTION_ID;
  return namespace ? `${namespace}-${name}` : name;
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
 *  It is also a `QueueAdmin`: `pause` and `resume` pause the function, and
 *  the dead jobs are its failed runs.
 *
 *  Needs Flow SDK 0.8.0 or later and an engine with the run control routes. */
export class FlowQueue implements Queue, QueueAdmin {
  private readonly functionId: string;

  constructor(private readonly flow: Flow, private readonly opts: FlowQueueOptions = {}) {
    this.functionId = functionName(opts.namespace, opts.functionId);
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
    // Only new work is refused: a retry or a resume belongs to a run that is
    // already under way, and refusing it would strand that run.
    if (this.opts.maxDepth && this.opts.maxDepth > 0 && !job.kind) {
      const counts = await this.flow.runs.counts({ function: this.functionId, ...this.env });
      if (counts.queued + counts.waiting_for_app + counts.paused >= this.opts.maxDepth) throw new QueueFullError();
    }

    // Twice at most: a singleton answer with no active run means the run that
    // held the key ended in between, so the key is free now.
    for (let i = 0; i < 2; i++) {
      // "hold": while the queue is paused, the job waits in it, as it would
      // in any queue, and starts on `resume`.
      const res = await this.flow.start(this.functionId, { data: data as never, whenPaused: 'hold', ...this.env });
      if (res.started) return;
      if (res.reason === 'singleton') {
        if (res.activeRunId === null) continue;
        if (opts?.key) throw new DuplicateJobError(opts.key);
        return;
      }
      if (res.reason === 'paused') throw new Error(`flow: function ${this.functionId} is paused`);
      throw new Error(`flow: no function ${this.functionId}; is the app served and synced?`);
    }
    // Both tries found the key held by a run that had just ended. No run
    // started, so the caller must hear it, or the job is lost.
    throw new Error(`flow: no run started for key ${key}; the key was still being freed`);
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
      this.flow.runs.list({ function: this.functionId, status: ['queued'], limit: 1, order: 'oldest', ...this.env }),
    ]);
    const oldest = queued.items[0];
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

  /** Pause the function. Jobs enqueued meanwhile wait, paused, and jobs
   *  already started pause at their next step. `resume` lets them all go. */
  async pause(): Promise<void> {
    await this.flow.functions.pause(this.functionId, this.env);
  }

  /** Resume the function and its paused runs. */
  async resume(): Promise<void> {
    await this.flow.functions.resume(this.functionId, this.env);
  }

  /** Cancel the waiting runs that match. Running runs are left alone. With
   *  `includeDead` it deletes the failed runs that match too. It reads Flow's
   *  runs index, so a job enqueued in the last few seconds can be missed. */
  async purge(filter: PurgeFilter): Promise<number> {
    if (!filter.threadId && !filter.runId && filter.kind === undefined && !(filter.olderThanMs && filter.olderThanMs > 0)) {
      throw new Error('flow queue: purge needs a filter');
    }
    const tags: Record<string, string> = {};
    if (filter.threadId) tags.threadId = filter.threadId;
    if (filter.runId) tags.runId = filter.runId;
    // The kind tag of a fresh dispatch is empty, which a tag filter cannot ask
    // for, so the kind is checked here.
    const match = (run: RunSummary) =>
      (filter.kind === undefined || (run.tags.kind ?? '') === (filter.kind ?? '')) &&
      (!filter.olderThanMs || Date.now() - run.createdAt > filter.olderThanMs);
    let n = 0;
    await this.eachRun(WAITING, tags, async (run) => {
      if (!match(run)) return;
      const res = await this.flow.runs.cancel({ runId: run.id }, this.env);
      n += res.cancelled.length;
    });
    if (!filter.includeDead) return n;
    await this.eachRun(['failed'], tags, async (run) => {
      if (!match(run)) return;
      if ((await this.flow.runs.delete(run.id, this.env)).deleted) n++;
    });
    return n;
  }

  /** The failed runs, newest first, with the job each carried. It asks each
   *  run for its error, so it makes one call per job. */
  async listDead(limit = 100): Promise<DeadJob[]> {
    if (limit <= 0) limit = 100;
    const out: DeadJob[] = [];
    await this.eachRun(['failed'], {}, async (run) => {
      if (out.length >= limit) return false;
      const detail = await this.flow.runs.get(run.id, this.env);
      // Gone since the list: deleted, or past its retention.
      if (!detail) return;
      const data = detail.event.data as unknown as FlowJobData | null;
      out.push({
        id: run.id,
        job: data?.job ?? ({} as RunJob),
        attempts: detail.attempts,
        deadAt: new Date(run.endedAt ?? 0),
        lastError: detail.error?.message ?? '',
      });
    });
    return out;
  }

  /** Retry a failed run from where it stopped, under the same run ID. False
   *  when there is no such failed run, the queue is paused, or another run
   *  holds the job's key now. */
  async redrive(id: string): Promise<boolean> {
    return (await this.flow.runs.retry(id, this.env)).retried;
  }

  /** Delete the failed runs that ended longer ago than `olderThanMs`. Runs
   *  kept for debugging in the dashboard stay. */
  async purgeDead(olderThanMs: number): Promise<number> {
    let n = 0;
    await this.eachRun(['failed'], {}, async (run) => {
      if (Date.now() - (run.endedAt ?? 0) <= olderThanMs) return;
      if ((await this.flow.runs.delete(run.id, this.env)).deleted) n++;
    });
    return n;
  }

  /** Calls `fn` for each of the function's runs in these statuses, newest
   *  first, page by page. `fn` gives back false to stop. */
  private async eachRun(
    status: RunStatus[], tags: Record<string, string>, fn: (run: RunSummary) => Promise<boolean | void>,
  ): Promise<void> {
    let before: string | undefined;
    for (;;) {
      const page = await this.flow.runs.list({
        function: this.functionId, status: status as ActiveRunStatus[], limit: PAGE_SIZE,
        ...(Object.keys(tags).length ? { tags } : {}), ...(before ? { before } : {}), ...this.env,
      });
      for (const run of page.items) {
        if ((await fn(run)) === false) return;
      }
      if (!page.next) return;
      before = page.next;
    }
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
   *  At most 56 characters with the namespace, since the failure function
   *  adds `-failure`. */
  id?: string;
  /** Goes in front of the ID. Must match `FlowQueue`'s `namespace`. */
  namespace?: string;
  /** How many jobs of one `partitionKey` run at once. Unset: no limit.
   *  Job priorities only take effect under a limit. */
  concurrency?: number;
  /** How many jobs run at once in all, whatever their `partitionKey`. Unset:
   *  no limit. It can be set with `concurrency`. */
  globalConcurrency?: number;
  /** Flow's retries for a job that throws. Flow's default when unset. */
  retries?: number;
  /** How long Flow waits for one job. Defaults to 14 minutes, Flow's most.
   *  When a job runs longer, Flow stops waiting and tries the step again,
   *  but nothing stops the first call: it keeps running. The retry finds the
   *  run lock held and does nothing, so keep segments shorter than this
   *  (`segmentTimeoutMs`). */
  stepTimeout?: Duration;
  /** The longest `segmentTimeoutMs` of the agents this function runs. When
   *  set, `agentRunFunction` throws if it is not shorter than `stepTimeout`,
   *  since a longer segment breaks Flow's retries. */
  segmentTimeoutMs?: number;
  /** The wait in ms before a failed job is tried again, doubled on every
   *  further failure up to `retryBackoffMaxMs`, as on the Postgres queue.
   *  Unset: Flow's own backoff. `retryBackoffMaxMs` defaults to one minute. */
  retryBackoffMs?: number;
  retryBackoffMaxMs?: number;
}

/** The wait before try `attempt + 1`: `base` doubled `attempt` times, up to
 *  `max`. */
export function retryBackoff(base: number, max: number, attempt: number): number {
  let d = base;
  for (let i = 0; i < attempt && d < max; i++) d *= 2;
  return Math.min(d, max);
}

/** A Flow duration in ms, read as Flow reads it: a number is ms, a string is
 *  like "14m" or "1h30m". NaN when it is not one. */
function durationMs(d: Duration): number {
  if (typeof d === 'number') return d;
  const unit: Record<string, number> = { ms: 1, s: 1000, m: 60_000, h: 3_600_000, d: 86_400_000, w: 604_800_000 };
  let total = 0;
  let consumed = 0;
  for (const m of d.trim().matchAll(/(\d+(?:\.\d+)?)(ms|s|m|h|d|w)/g)) {
    total += Number(m[1]) * unit[m[2]!]!;
    consumed += m[0].length;
  }
  return consumed > 0 && consumed === d.trim().length ? total : Number.NaN;
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
  const stepTimeout = opts.stepTimeout ?? '14m';
  if (opts.segmentTimeoutMs && opts.segmentTimeoutMs > 0 && !(opts.segmentTimeoutMs < durationMs(stepTimeout))) {
    throw new Error(
      `flow: segmentTimeoutMs ${opts.segmentTimeoutMs} must be shorter than stepTimeout ${String(stepTimeout)}, or a long segment is tried again while it still runs`,
    );
  }
  const backoffMax = opts.retryBackoffMaxMs && opts.retryBackoffMaxMs > 0 ? opts.retryBackoffMaxMs : 60_000;
  const concurrency = [
    ...(opts.concurrency ? [{ limit: opts.concurrency, key: 'event.data.job.partitionKey' }] : []),
    ...(opts.globalConcurrency ? [{ limit: opts.globalConcurrency }] : []),
  ];
  return flow.createFunction(
    {
      id: functionName(opts.namespace, opts.id),
      singleton: { key: 'event.data.key', mode: 'skip' },
      tags: { runId: 'event.data.job.runId', threadId: 'event.data.job.threadId', kind: 'event.data.job.kind' },
      priority: { run: 'event.data.priority' },
      ...(concurrency.length ? { concurrency } : {}),
      ...(opts.retries !== undefined ? { retries: opts.retries } : {}),
      timeouts: { step: stepTimeout },
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
      return step.run('handle', async ({ attempt }) => {
        try {
          return await core.worker.handleJob(data.job);
        } catch (err) {
          if (!opts.retryBackoffMs || opts.retryBackoffMs <= 0) throw err;
          throw new RetryAfterError(err instanceof Error ? err.message : String(err),
            Date.now() + retryBackoff(opts.retryBackoffMs, backoffMax, attempt), { cause: err });
        }
      });
    },
  );
}

// ---- names and big values, shared by the bus and the streams ----

/** Flow's charset for channel and stream names. */
const NAME_RE = /^[A-Za-z0-9:_.-]{1,256}$/;

/** Under Flow's 64 KB cap on one message or one stream item, with room for
 *  the rest of the request. A value over it goes through the kv. */
const MAX_INLINE_BYTES = 60_000;

const byteLength = (s: string) => new TextEncoder().encode(s).length;

async function sha256Hex(s: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(s));
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

/** `prefix + id` when Flow allows it, else `prefix + "h." + sha256(id)`. */
async function flowName(prefix: string, id: string): Promise<string> {
  const name = prefix + id;
  if (NAME_RE.test(name) && name !== '.' && name !== '..' && name !== 'token' && name !== 'connect') return name;
  return `${prefix}h.${await sha256Hex(id)}`;
}

// ---- FlowBus ----

/** The channel a thread's events go on. */
export function flowThreadChannel(threadId: string): Promise<string> {
  return flowName('agentenkit.thread.', threadId);
}

const BUS_TOPIC = 'events';

/** How long an event too big for one message waits in the kv. */
const BUS_REF_SECONDS = 60;

/** What travels on the channel: the event, or the kv key that holds it. */
type BusFrame = { event: AgentEvent } | { ref: string };

export interface FlowBusOptions {
  /** Holds events too big for one realtime message (over about 60 KB). */
  kv: Kv;
  /** How often each subscriber gets a HEARTBEAT notice. Default one minute. */
  heartbeatMs?: number;
  /** How long `subscribe` waits for its socket to open before it returns
   *  anyway. Default 10 seconds. */
  openTimeoutMs?: number;
  /** Cloudflare Workers: the handler's `env`, where the Flow keys live. */
  env?: FlowEnv;
  /** A WebSocket class, where the runtime has none. */
  WebSocket?: typeof WebSocket;
}

interface BusChannel {
  handlers: Set<(event: AgentEvent) => void>;
  stop: () => void;
  open: Promise<void>;
  /** Set when the socket could not be started: `subscribe` throws it. */
  failed?: { error: unknown };
  /** The frames being read, one after another, so events keep their order
   *  even when one waits on the kv. */
  queue: Promise<void>;
}

/** EventBus over Clevix Flow realtime.
 *
 *  Each thread has its own channel, and a process opens one socket per thread
 *  it follows, shared by every subscriber in the process. Realtime keeps
 *  nothing and drops what comes while a socket reconnects: the bus is
 *  at-most-once anyway, and a follower fills any gap from the log. An event
 *  over about 60 KB waits in the kv for a minute, and the channel carries its
 *  key. While anyone subscribes, each subscriber gets a HEARTBEAT notice
 *  (seq 0) every `heartbeatMs`, as with the other buses.
 *
 *  Needs Flow SDK 0.7.0 or later. */
export class FlowBus implements EventBus {
  private readonly channels = new Map<string, BusChannel>();
  private heartbeat?: ReturnType<typeof setInterval>;

  constructor(private readonly flow: Flow, private readonly opts: FlowBusOptions) {}

  private get env(): { env?: FlowEnv } {
    return this.opts.env ? { env: this.opts.env } : {};
  }

  async publish(threadId: string, event: AgentEvent): Promise<void> {
    const raw = JSON.stringify(event);
    let frame: BusFrame = { event };
    if (byteLength(raw) > MAX_INLINE_BYTES) {
      const ref = `agentenkit:flowbus:${threadId}:${crypto.randomUUID()}`;
      await this.opts.kv.set(ref, raw, { exSeconds: BUS_REF_SECONDS });
      frame = { ref };
    }
    const channel = await flowThreadChannel(threadId);
    await this.flow.realtime.publish({ channel, topic: BUS_TOPIC, data: frame as unknown as Json }, this.env);
  }

  async subscribe(threadId: string, handler: (event: AgentEvent) => void): Promise<() => void> {
    let ch = this.channels.get(threadId);
    if (!ch) ch = this.connect(threadId);
    ch.handlers.add(handler);
    this.startHeartbeat();
    await ch.open;
    if (ch.failed) {
      ch.handlers.delete(handler);
      if (this.channels.size === 0) this.stopHeartbeat();
      throw ch.failed.error;
    }

    let done = false;
    const mine = ch;
    return () => {
      if (done) return;
      done = true;
      mine.handlers.delete(handler);
      if (mine.handlers.size === 0 && this.channels.get(threadId) === mine) {
        this.channels.delete(threadId);
        mine.stop();
      }
      if (this.channels.size === 0) this.stopHeartbeat();
    };
  }

  /** Opens the thread's socket. `open` settles when it is open, or after
   *  `openTimeoutMs`: an event published before then may be missed, which a
   *  follower's gap read covers. */
  private connect(threadId: string): BusChannel {
    let opened!: () => void;
    const open = new Promise<void>((resolve) => (opened = resolve));
    const timer = setTimeout(opened, this.opts.openTimeoutMs ?? 10_000);
    (timer as unknown as { unref?: () => void }).unref?.();
    let stop: (() => void) | undefined;
    let stopped = false;
    const ch: BusChannel = {
      handlers: new Set(),
      open,
      queue: Promise.resolve(),
      stop: () => {
        stopped = true;
        clearTimeout(timer);
        opened();
        stop?.();
      },
    };
    this.channels.set(threadId, ch);
    flowThreadChannel(threadId)
      .then((channel) => {
        if (stopped) return;
        stop = this.flow.realtime.subscribe<BusFrame>({
          channel,
          topics: [BUS_TOPIC],
          ...this.env,
          ...(this.opts.WebSocket ? { WebSocket: this.opts.WebSocket } : {}),
          onState: (state) => {
            if (state === 'open') {
              clearTimeout(timer);
              opened();
            }
          },
          onMessage: (message) => {
            ch.queue = ch.queue.then(() => this.receive(ch, message.data));
          },
        });
      })
      .catch((error: unknown) => {
        // No socket (no WebSocket class, bad options): every subscriber
        // waiting on it gets the error, as Go's Subscribe returns it.
        ch.failed = { error };
        if (this.channels.get(threadId) === ch) this.channels.delete(threadId);
        clearTimeout(timer);
        opened();
      });
    return ch;
  }

  private async receive(ch: BusChannel, frame: BusFrame) {
    let event: AgentEvent;
    try {
      if ('event' in frame) {
        event = frame.event;
      } else {
        const raw = await this.opts.kv.get(frame.ref);
        if (raw === null) return; // expired: the follower reads it from the log
        event = JSON.parse(raw) as AgentEvent;
      }
      event.createdAt = new Date(event.createdAt);
    } catch {
      return; // a malformed frame never kills the subscription
    }
    this.deliver(ch.handlers, event);
  }

  private deliver(handlers: Iterable<(event: AgentEvent) => void>, event: AgentEvent) {
    for (const handler of [...handlers]) {
      try {
        handler(event);
      } catch {
        // one throwing handler must not stop the others
      }
    }
  }

  private startHeartbeat() {
    if (this.heartbeat) return;
    this.heartbeat = setInterval(() => {
      for (const [threadId, ch] of this.channels) {
        this.deliver(ch.handlers, { threadId, seq: 0, type: 'HEARTBEAT', payload: null, createdAt: new Date() } as AgentEvent);
      }
    }, this.opts.heartbeatMs ?? 60_000);
    (this.heartbeat as unknown as { unref?: () => void }).unref?.();
  }

  private stopHeartbeat() {
    if (this.heartbeat) clearInterval(this.heartbeat);
    this.heartbeat = undefined;
  }
}

// ---- FlowStreams ----

/** The Flow stream a run stream is kept in. */
export function flowStreamId(streamId: string): Promise<string> {
  return flowName('agentenkit.', streamId);
}

/** Flow's limits on one stream call. */
const MAX_APPEND_ITEMS = 100;
const MAX_APPEND_BYTES = 900_000;
const MAX_STREAM_MS = 7 * 24 * 60 * 60_000;

/** An item too big for Flow: the kv key that holds the event. */
interface StreamRef {
  agentenkitRef: string;
}

export interface FlowStreamsOptions {
  /** Holds items too big for one stream item (over about 60 KB). */
  kv: Kv;
  /** How long such an item stays in the kv. Default 8 days: past Flow's
   *  longest time to live, so it outlives its stream. */
  refTtlMs?: number;
  /** Cloudflare Workers: the handler's `env`, where the Flow keys live. */
  env?: FlowEnv;
  /** A WebSocket class, where the runtime has none. */
  WebSocket?: typeof WebSocket;
}

/** RunStreams over Clevix Flow streams.
 *
 *  Flow keeps the items, gives offsets, and lets a reader start from any
 *  offset and follow live, which is what a run stream needs. An item over
 *  about 60 KB is kept in the kv and the stream carries its key; readers get
 *  the event back. Flow keeps a stream at most 7 days and a grace window at
 *  most 7 days, so longer times are cut to that. A stream holds at most
 *  20,000 items and 32 MB; an append past that fails.
 *
 *  Needs Flow SDK 0.7.0 or later. */
export class FlowStreams implements RunStreams {
  constructor(private readonly flow: Flow, private readonly opts: FlowStreamsOptions) {}

  private get env(): { env?: FlowEnv } {
    return this.opts.env ? { env: this.opts.env } : {};
  }

  async open(streamId: string, meta: StreamMeta, ttlMs: number): Promise<void> {
    const id = await flowStreamId(streamId);
    const ttl = Math.min(MAX_STREAM_MS, Math.max(1000, Math.ceil(ttlMs)));
    await this.flow.streams.open(id, { meta: { threadId: meta.threadId, runId: meta.runId }, ttl, ...this.env });
  }

  async append(streamId: string, events: StreamEvent[]): Promise<string[]> {
    if (events.length === 0) return [];
    const id = await flowStreamId(streamId);
    const items: Array<{ json: Json; bytes: number }> = [];
    for (const event of events) {
      const raw = JSON.stringify(event);
      if (byteLength(raw) <= MAX_INLINE_BYTES) {
        items.push({ json: event as unknown as Json, bytes: byteLength(raw) });
        continue;
      }
      const ref: StreamRef = { agentenkitRef: `agentenkit:flowstream:${streamId}:${crypto.randomUUID()}` };
      await this.opts.kv.set(ref.agentenkitRef, raw, { exSeconds: Math.ceil((this.opts.refTtlMs ?? 8 * 24 * 60 * 60_000) / 1000) });
      items.push({ json: ref as unknown as Json, bytes: byteLength(JSON.stringify(ref)) });
    }
    // In batches Flow takes: 100 items and under 1 MB a call.
    const offsets: string[] = [];
    let batch: Json[] = [];
    let size = 0;
    const flush = async () => {
      if (batch.length === 0) return;
      offsets.push(...(await this.call(streamId, () => this.flow.streams.append(id, batch, this.env))));
      batch = [];
      size = 0;
    };
    for (const item of items) {
      if (batch.length === MAX_APPEND_ITEMS || size + item.bytes > MAX_APPEND_BYTES) await flush();
      batch.push(item.json);
      size += item.bytes;
    }
    await flush();
    return offsets;
  }

  async *read(streamId: string, after: string | null, signal?: AbortSignal): AsyncIterable<StreamItem> {
    const id = await flowStreamId(streamId);
    const items = this.flow.streams.read<Json>(id, {
      ...(after ? { after } : {}),
      ...(signal ? { signal } : {}),
      ...(this.opts.WebSocket ? { WebSocket: this.opts.WebSocket } : {}),
      ...this.env,
    });
    try {
      for await (const item of items) {
        const decoded = await this.decode(item);
        if (decoded) yield decoded;
      }
    } catch (err) {
      throw mapStreamError(streamId, err);
    }
  }

  async snapshot(streamId: string, after?: string | null): Promise<StreamSnapshot | null> {
    const id = await flowStreamId(streamId);
    const snap = await this.call(streamId, () => this.flow.streams.snapshot<Json>(id, { ...(after ? { after } : {}), ...this.env }));
    if (!snap) return null;
    const items: StreamItem[] = [];
    for (const item of snap.items) {
      const decoded = await this.decode(item);
      if (decoded) items.push(decoded);
    }
    // The end item is the last one; `end` is the event, without its offset.
    let end: StreamEnd | null = null;
    const last = items[items.length - 1];
    if (snap.ended && last && isStreamEnd(last)) {
      const { offset: _, ...event } = last;
      end = event as StreamEnd;
    }
    const meta = snap.meta as unknown as StreamMeta | null;
    return {
      meta: { threadId: meta?.threadId ?? '', runId: meta?.runId ?? '' },
      items,
      end,
    };
  }

  async close(streamId: string, end: StreamEnd, graceMs: number): Promise<void> {
    const id = await flowStreamId(streamId);
    const grace = Math.min(MAX_STREAM_MS, Math.max(0, Math.ceil(graceMs)));
    await this.call(streamId, () => this.flow.streams.close(id, { end: end as unknown as Json, grace, ...this.env }));
  }

  async delete(streamId: string): Promise<void> {
    const id = await flowStreamId(streamId);
    await this.flow.streams.delete(id, this.env);
  }

  private async call<T>(streamId: string, fn: () => Promise<T>): Promise<T> {
    try {
      return await fn();
    } catch (err) {
      throw mapStreamError(streamId, err);
    }
  }

  /** A Flow item as a run stream item: the event, read back from the kv when
   *  the stream holds its key. Null when that kv copy has expired. */
  private async decode(item: FlowStreamItem<Json>): Promise<StreamItem | null> {
    let event = item.item as unknown as StreamEvent | StreamRef;
    if (event && typeof event === 'object' && 'agentenkitRef' in event) {
      const raw = await this.opts.kv.get(event.agentenkitRef);
      if (raw === null) return null;
      event = JSON.parse(raw) as StreamEvent;
    }
    return { ...(event as StreamEvent), offset: item.offset } as StreamItem;
  }
}

/** Flow's stream errors as the port's, by name: the adapter does not load
 *  `@clevix/flow` itself. */
function mapStreamError(streamId: string, err: unknown): unknown {
  const name = (err as { name?: string } | null)?.name;
  if (name === 'StreamGoneError') return new StreamGoneError(streamId);
  if (name === 'StreamClosedError') return new StreamClosedError(streamId);
  return err;
}
