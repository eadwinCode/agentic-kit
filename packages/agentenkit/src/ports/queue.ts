import type { RunJob } from '../core/types.js';

/** The priority of the platform's own housekeeping: park expiries, reclaims
 *  and lock redrives. A user's message is never queued behind them. */
export const PRIORITY_LOW = -10;

/** `enqueue` refused a fresh dispatch: a depth cap is set and the queue has
 *  reached it. Only a fresh dispatch is ever refused; retries, redrives,
 *  resumes and expiries always go in, or a run already under way would be
 *  stranded. */
export class QueueFullError extends Error {
  constructor(message = 'queue is full') {
    super(message);
    this.name = 'QueueFullError';
  }
}

/** A job whose encoded payload is over the adapter's cap. */
export class PayloadTooLargeError extends Error {
  constructor(message = 'job payload too large') {
    super(message);
    this.name = 'PayloadTooLargeError';
  }
}

/** An enqueue whose `key` is already waiting or running. */
export class DuplicateJobError extends Error {
  constructor(key: string) {
    super(`a job with this key is already queued: ${key}`);
    this.name = 'DuplicateJobError';
  }
}

/** An adapter's answer to an operation it cannot do, such as counting on a
 *  queue with no read side. Callers treat it as "unknown", never as a
 *  failure. */
export class UnsupportedError extends Error {
  constructor(what: string) {
    super(`not supported by this adapter: ${what}`);
    this.name = 'UnsupportedError';
  }
}

export interface EnqueueOptions {
  /** Hold the job for this many seconds before delivering it: the HITL expiry
   *  (§2.5), a job blocked by an older run's lock (§2.8) and a failed run's
   *  retry backoff.
   *
   *  An adapter that cannot delay may deliver immediately — every caller
   *  treats an early arrival as a no-op — but it must NOT throw, or a park
   *  would fail the run that scheduled it. Note the shape of the reference
   *  adapter: QStash supports delays on publish only and rejects them on
   *  queue enqueue, so QStashQueue publishes delayed jobs and queues the
   *  rest. */
  delaySeconds?: number;
  /** Dedupes. At most one live job per key: a second enqueue with the same
   *  key is refused with `DuplicateJobError`, and `cancel` drops the waiting
   *  job for a key. Omitted means no dedupe. */
  key?: string;
  /** Orders ready jobs: higher first, then oldest first. 0 is a normal
   *  dispatch; the platform's housekeeping uses `PRIORITY_LOW`. */
  priority?: number;
}

/** What a queue can say about itself. */
export interface QueueStats {
  /** Jobs a worker could take right now. */
  ready: number;
  /** Jobs held for a future run time: park expiries, retry backoffs. Not
   *  backlog. */
  delayed: number;
  /** Jobs a worker holds. */
  inFlight: number;
  /** Jobs the queue gave up on and kept for an operator. */
  dead: number;
  /** How long the oldest ready job has been waiting: the number that says a
   *  queue is stuck rather than merely busy. */
  oldestReadyMs: number;
  /** True while the consumer is told not to claim. */
  paused: boolean;
  /** When this process last took a job. */
  lastClaimAt?: Date;
  /** How many claims in a row have failed in this process. */
  claimErrors: number;
}

/** One job as the queue sees it, for a host that asks where a run's job is. */
export interface QueuedJob {
  id: string;
  runId: string;
  threadId: string;
  kind: RunJob['kind'];
  attempts: number;
  runAt: Date;
  lockedUntil?: Date;
  /** How many ready jobs are ahead of it; -1 when the adapter cannot say. */
  position: number;
}

/** Durable dispatch port (§2.8). Delivery is at-least-once — the engine's
 *  state guard and the run lock make double dispatch a no-op.
 *
 *  Beside `enqueue` the port carries the little an engine and a host need to
 *  see the queue: a way to withdraw a job by key, a way to find a run's job,
 *  and counts. An adapter with no read side throws `UnsupportedError` from
 *  `find` and `stats`; callers treat that as "unknown", never as a failure. */
export interface Queue {
  enqueue(job: RunJob, opts?: EnqueueOptions): Promise<void>;
  /** Drop every waiting job enqueued under `key`. A key nothing is waiting
   *  under is not an error. Best-effort by contract: a delivered job is a
   *  correct no-op anyway. */
  cancel(key: string): Promise<void>;
  /** The earliest waiting or running job for a run, or null when the queue
   *  holds none. */
  find(runId: string): Promise<QueuedJob | null>;
  /** Count what waits, runs and died. */
  stats(): Promise<QueueStats>;
}
