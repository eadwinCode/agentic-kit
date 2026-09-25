import type { RunJob } from '../core/types.js';
import type { EnqueueOptions, Queue } from '../ports/queue.js';

/** An in-process queue that actually dispatches (§2.8).
 *
 *  `MemoryQueue` only collects jobs, which is right for tests that drive the
 *  worker by hand but useless for running the platform. This one hands each
 *  job to the worker on a later tick, so `enqueue` still returns immediately
 *  and a run keeps outliving the request that started it — the property the
 *  whole design rests on.
 *
 *  It honours `delaySeconds`, so the HITL expiry (§2.5) and the lock-conflict
 *  redrive (§2.8) work in development exactly as they do against a real queue.
 *
 *  What it is NOT: durable. A process restart loses whatever was in flight,
 *  which is precisely what the durable adapters exist to fix. Development
 *  only. */
export class InlineQueue implements Queue {
  private handler?: (job: RunJob) => Promise<unknown>;
  private readonly pending = new Set<ReturnType<typeof setTimeout>>();
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
    const timer = setTimeout(() => {
      this.pending.delete(timer);
      this.deliver(job);
    }, (opts?.delaySeconds ?? 0) * 1000);
    // Never hold a process open just because an expiry is scheduled.
    (timer as unknown as { unref?: () => void }).unref?.();
    this.pending.add(timer);
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

  /** Drop everything still scheduled — for tests and clean shutdown. */
  clear(): void {
    for (const t of this.pending) clearTimeout(t);
    this.pending.clear();
    this.unbound.length = 0;
  }
}
