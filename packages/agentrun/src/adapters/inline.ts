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

  /** Wired by setupAgentCore once the worker exists — the queue and the worker
   *  each need the other, so one of them has to be attached afterwards. */
  bind(handler: (job: RunJob) => Promise<unknown>): void {
    this.handler = handler;
  }

  async enqueue(job: RunJob, opts?: EnqueueOptions): Promise<void> {
    const timer = setTimeout(() => {
      this.pending.delete(timer);
      // Detached on purpose: a queue consumer's failure is the worker's
      // business (§2.8 redrive), never the enqueuer's.
      void this.handler?.(job)?.catch(() => undefined);
    }, (opts?.delaySeconds ?? 0) * 1000);
    // Never hold a process open just because an expiry is scheduled.
    (timer as unknown as { unref?: () => void }).unref?.();
    this.pending.add(timer);
  }

  /** Drop everything still scheduled — for tests and clean shutdown. */
  clear(): void {
    for (const t of this.pending) clearTimeout(t);
    this.pending.clear();
  }
}
