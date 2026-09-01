import type { EnqueueOptions, Queue } from '../ports/queue.js';
import type { RunJob } from '../core/types.js';

/** Minimal structural type of the @upstash/qstash (v2) client we use. */
export interface QStashLike {
  queue(a: { queueName: string }): {
    enqueueJSON(a: { url: string; body: unknown }): Promise<unknown>;
  };
  /** `delay` is in seconds. QStash supports it on publish only — an enqueue
   *  carrying Upstash-Delay is REJECTED outright ("Upstash-Not-Before/
   *  Upstash-Delay can not be used with enqueue"), so a delayed dispatch has
   *  to be published instead. */
  publishJSON(a: { url: string; body: unknown; delay?: number }): Promise<unknown>;
}

export interface QStashQueueOptions {
  /** Fully-qualified consumer URL, e.g. https://app.example.com/api/queue/agent-run */
  url: string;
  /** Queue name for flow control (§2.8). Defaults to `agent-runs`. */
  queueName?: string;
}

/** Reference Queue adapter over Upstash QStash HTTP queues. */
export class QStashQueue implements Queue {
  constructor(private readonly client: QStashLike, private readonly opts: QStashQueueOptions) {}

  enqueue(job: RunJob, opts?: EnqueueOptions): Promise<void> {
    // A delayed job goes out as a published message rather than a queued one:
    // QStash rejects Upstash-Delay on enqueue. The trade is that this one
    // message skips the queue's flow control — acceptable for the two things
    // that ask for a delay (a HITL expiry and a blocked job's redrive), since
    // both are single messages the run lock already serializes.
    if (opts?.delaySeconds) {
      return this.client
        .publishJSON({ url: this.opts.url, body: job, delay: opts.delaySeconds })
        .then(() => undefined);
    }

    return this.client
      .queue({ queueName: this.opts.queueName ?? 'agent-runs' })
      .enqueueJSON({ url: this.opts.url, body: job })
      .then(() => undefined);
  }
}
