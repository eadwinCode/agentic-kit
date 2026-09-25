import type { RunJob } from '../core/types.js';
import {
  DuplicateJobError,
  type EnqueueOptions,
  type Queue,
  type QueueStats,
  type QueuedJob,
} from '../ports/queue.js';

/** The longest delay one timer can hold: past about 24.8 days a timer fires
 *  at once. A longer delay is waited out in steps of this. */
const MAX_TIMER_MS = 2 ** 31 - 1;

interface Pending {
  job: RunJob;
  key?: string;
  runAt: number;
  timer?: ReturnType<typeof setTimeout>;
}

/** An in-process queue that actually dispatches (§2.8).
 *
 *  `MemoryQueue` only collects jobs, which is right for tests that drive the
 *  worker by hand but useless for running the platform. This one hands each
 *  job to the worker on a later tick, so `enqueue` still returns immediately
 *  and a run keeps outliving the request that started it — the property the
 *  whole design rests on.
 *
 *  It honours `delaySeconds`, so the HITL expiry (§2.5) and the lock-conflict
 *  redrive (§2.8) work in development exactly as they do against a real queue,
 *  however long the delay. A `key` dedupes against the jobs still waiting,
 *  and `cancel` withdraws them.
 *
 *  What it is NOT: durable. A process restart loses whatever was in flight,
 *  which is precisely what the durable adapters exist to fix. Development
 *  only. */
export class InlineQueue implements Queue {
  private handler?: (job: RunJob) => Promise<unknown>;
  private readonly pending = new Set<Pending>();
  /** Jobs that came due before `bind`: held, not dropped. */
  private readonly unbound: RunJob[] = [];

  /** Attach the worker once the runtime exists — the queue and the worker each
   *  need the other, so one of them has to be wired afterwards:
   *  `queue.bind((job) => runtime.worker.handleJob(job))`. A job that came
   *  due before this runs is delivered now rather than lost. */
  bind(handler: (job: RunJob) => Promise<unknown>): void {
    this.handler = handler;
    for (const job of this.unbound.splice(0)) this.deliver(job);
  }

  async enqueue(job: RunJob, opts?: EnqueueOptions): Promise<void> {
    if (opts?.key && [...this.pending].some((p) => p.key === opts.key)) {
      throw new DuplicateJobError(opts.key);
    }
    const p: Pending = { job, key: opts?.key, runAt: Date.now() + (opts?.delaySeconds ?? 0) * 1000 };
    this.pending.add(p);
    this.arm(p);
  }

  /** Set the job's timer, in steps no longer than one timer can hold. */
  private arm(p: Pending): void {
    const wait = Math.max(0, p.runAt - Date.now());
    p.timer = setTimeout(() => {
      if (Date.now() < p.runAt) return this.arm(p); // a long delay, not due yet
      this.pending.delete(p);
      this.deliver(p.job);
    }, Math.min(wait, MAX_TIMER_MS));
    // Never hold a process open just because an expiry is scheduled.
    (p.timer as unknown as { unref?: () => void }).unref?.();
  }

  async cancel(key: string): Promise<void> {
    if (!key) return;
    for (const p of this.pending) {
      if (p.key !== key) continue;
      clearTimeout(p.timer);
      this.pending.delete(p);
    }
  }

  async find(runId: string): Promise<QueuedJob | null> {
    let found: Pending | undefined;
    for (const p of this.pending) {
      if (runId && p.job.runId === runId && (!found || p.runAt < found.runAt)) found = p;
    }
    return found
      ? {
          id: '', runId, threadId: found.job.threadId, kind: found.job.kind, attempts: 0,
          runAt: new Date(found.runAt), position: -1,
        }
      : null;
  }

  async stats(): Promise<QueueStats> {
    const now = Date.now();
    let delayed = 0;
    for (const p of this.pending) if (p.runAt > now) delayed++;
    return {
      ready: this.pending.size - delayed + this.unbound.length, delayed, inFlight: 0, dead: 0,
      oldestReadyMs: 0, paused: false, claimErrors: 0,
    };
  }

  private deliver(job: RunJob): void {
    if (!this.handler) {
      this.unbound.push(job);
      return;
    }
    // Detached on purpose: a queue consumer's failure is the worker's
    // business (§2.8 redrive), never the enqueuer's. It is still logged:
    // swallowed, a broken worker looks like a queue that does nothing.
    void Promise.resolve()
      .then(() => this.handler!(job))
      .catch((err) =>
        console.error('InlineQueue: job failed', { threadId: job.threadId, runId: job.runId }, err),
      );
  }

  /** Drop everything still scheduled — for tests and clean shutdown. What it
   *  drops is logged: a job lost without a trace looks like a queue that does
   *  nothing. */
  clear(): void {
    const dropped = this.pending.size + this.unbound.length;
    for (const p of this.pending) clearTimeout(p.timer);
    this.pending.clear();
    this.unbound.length = 0;
    if (dropped > 0) console.warn(`InlineQueue: cleared with ${dropped} job(s) still waiting`);
  }
}
