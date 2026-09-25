import { randomUUID } from 'node:crypto';
import type { RuntimePorts } from '../ports/runtime.js';
import type { EnqueueOptions } from '../ports/queue.js';
import type { RunJob } from './types.js';

/** The one way the platform puts a job on the queue: it stamps the job with a
 *  fresh `dispatchId`, so every enqueue is a delivery of its own and only the
 *  queue's own redelivery repeats one (§3.4). */
export function enqueueJob(deps: RuntimePorts, job: RunJob, opts?: EnqueueOptions): Promise<void> {
  return deps.queue.enqueue({ ...job, dispatchId: job.dispatchId ?? randomUUID() }, opts);
}

/** The per-thread run lock (§3.4). */
export const runLockKey = (threadId: string) => `agent:lock:${threadId}`;

/** The run lock's value is `<runId>/<dispatchId>/<nonce>` (§3.4).
 *
 *  - `runId` says which run holds the thread.
 *  - `dispatchId` says which delivery of that run. It rides on the job, so a
 *    queue that delivers the same job twice delivers the same `dispatchId`,
 *    and the second copy is known for a duplicate.
 *  - `nonce` is new on every acquire. Two holders of the same job — a stalled
 *    worker whose lease lapsed, and the redelivery that took it over — differ
 *    here and nowhere else, so the stalled one can neither renew nor free the
 *    new holder's lock.
 *
 *  A value written before dispatch ids existed is a bare run id; it parses as
 *  that run with no dispatch. */
const SEP = '/';

export function parseLockValue(value: string | null): { runId: string | null; dispatchId: string | null } {
  if (value === null) return { runId: null, dispatchId: null };
  const parts = value.split(SEP);
  if (parts.length < 3) return { runId: value, dispatchId: null };
  return { runId: parts[0], dispatchId: parts[1] };
}

/** Thrown out of a loop whose lease was lost between steps: another worker
 *  may own the thread, so this one must not write another step to it. */
export class RunLockLostError extends Error {
  constructor() {
    super('run lock lost');
    this.name = 'RunLockLostError';
  }
}

/** One holder's grip on a thread's run lock. */
export class Lease {
  private lostFlag = false;
  private timer?: ReturnType<typeof setInterval>;
  private released = false;

  private constructor(
    private readonly deps: RuntimePorts,
    private readonly key: string,
    private readonly value: string,
  ) {}

  /** Take the thread's run lock for a run and one delivery of it; null when
   *  someone else holds it. */
  static async acquire(
    deps: RuntimePorts,
    threadId: string,
    runId: string | undefined,
    dispatchId: string | undefined,
  ): Promise<Lease | null> {
    const value = [runId ?? randomUUID(), dispatchId ?? randomUUID(), randomUUID()].join(SEP);
    const lease = new Lease(deps, runLockKey(threadId), value);
    const ok = await deps.kv.set(lease.key, value, {
      onlyIfNotExists: true,
      exSeconds: deps.config.runLockLeaseSeconds,
    });
    return ok ? lease : null;
  }

  /** The lease could not be kept. Another worker may own the thread by now,
   *  so the holder must stop writing to it. */
  get lost(): boolean {
    return this.lostFlag;
  }

  /** Renew in the background until `release`, the way a queue renews a job
   *  lease: an expired lock then means a dead worker and nothing else.
   *  `onLost` runs once, when the lease cannot be kept — the key is gone or
   *  holds another holder's value, or no renewal has landed for two thirds of
   *  the lease (it is then close to lapsing, and another worker may take it
   *  before the next try). */
  keep(onLost?: () => void): void {
    if (this.timer || this.released) return;
    const leaseMs = this.deps.config.runLockLeaseSeconds * 1000;
    const everyMs = Math.max(Math.floor(leaseMs / 6), 1);
    const log = this.deps.log ?? console;
    let lastOk = Date.now();
    let inFlight = false;
    this.timer = setInterval(async () => {
      if (inFlight || this.lostFlag || this.released) return;
      inFlight = true;
      let outcome: 'ok' | 'taken' | 'error' = 'error';
      let error: unknown;
      try {
        // Bounded: a call that hangs past the next tick counts as a failure,
        // not as time silently spent holding a lease that may be lapsing.
        const renewed = await Promise.race([
          this.deps.kv.setIfValue(this.key, this.value, this.value, {
            exSeconds: this.deps.config.runLockLeaseSeconds,
          }),
          new Promise<never>((_, reject) =>
            setTimeout(() => reject(new Error('renewal timed out')), everyMs).unref?.(),
          ),
        ]);
        outcome = renewed ? 'ok' : 'taken';
      } catch (err) {
        error = err;
      } finally {
        inFlight = false;
      }
      if (this.released) return;
      if (outcome === 'ok') {
        lastOk = Date.now();
        return;
      }
      if (outcome === 'error' && Date.now() - lastOk <= (leaseMs * 2) / 3) {
        // The Logger port only promises `error`; a transient miss is not one.
        (log as { warn?: (m: string, ...r: unknown[]) => void }).warn?.(
          'run lock renewal failed; retrying',
          { key: this.key, err: String(error) },
        );
        return;
      }
      log.error(
        outcome === 'taken'
          ? 'run lock taken by another holder; ending the segment'
          : 'run lock could not be renewed in time; ending the segment',
        { key: this.key, ...(error ? { err: String(error) } : {}) },
      );
      this.lostFlag = true;
      this.stop();
      onLost?.();
    }, everyMs);
    // Never hold a process open just to renew a lock.
    (this.timer as unknown as { unref?: () => void }).unref?.();
  }

  /** Stop renewing and free the lock, only while it is still this holder's.
   *  Safe to call more than once. */
  async release(): Promise<void> {
    if (this.released) return;
    this.released = true;
    this.stop();
    await this.deps.kv.delIfValue(this.key, this.value).catch(() => undefined);
  }

  private stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
  }
}
