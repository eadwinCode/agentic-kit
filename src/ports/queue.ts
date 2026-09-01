import type { RunJob } from '../core/types.js';

export interface EnqueueOptions {
  /** Hold the job for this many seconds before delivering it: the HITL expiry
   *  (§2.5) and a job blocked by an older run's lock (§2.8).
   *
   *  An adapter that cannot delay may deliver immediately — both callers treat
   *  an early arrival as a no-op — but it must NOT throw, or a park would fail
   *  the run that scheduled it. Note the shape of the reference adapter: QStash
   *  supports delays on publish only and rejects them on queue enqueue, so
   *  QStashQueue publishes delayed jobs and queues the rest. */
  delaySeconds?: number;
}

/** Durable dispatch port (§2.8). Delivery is at-least-once — the engine's
 *  state guard + Storage.threads.claimState make double dispatch a no-op. */
export interface Queue {
  enqueue(job: RunJob, opts?: EnqueueOptions): Promise<void>;
}
