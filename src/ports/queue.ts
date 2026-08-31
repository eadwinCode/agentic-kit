import type { RunJob } from '../core/types.js';

/** Durable dispatch port (§2.8). Delivery is at-least-once — the engine's
 *  state guard + Storage.threads.claimState make double dispatch a no-op. */
export interface Queue {
  enqueue(job: RunJob): Promise<void>;
}
