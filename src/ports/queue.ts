import type { RunJob } from '../core/types.js';

export interface EnqueueOptions {
  /** Hold the job for this many seconds before delivering it. Used to let a
   *  previous run release the per-thread lock (§2.8). An adapter that cannot
   *  delay may deliver immediately — the run id keeps that correct, just
   *  busier. */
  delaySeconds?: number;
}

/** Durable dispatch port (§2.8). Delivery is at-least-once — the engine's
 *  state guard + Storage.threads.claimState make double dispatch a no-op. */
export interface Queue {
  enqueue(job: RunJob, opts?: EnqueueOptions): Promise<void>;
}
